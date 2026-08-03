package io.jobrunr.docsmcp.web;

import io.jobrunr.docsmcp.index.DocsRegistry;
import io.jobrunr.docsmcp.index.LuceneIndex;
import io.jobrunr.docsmcp.index.VectorIndex;
import io.jobrunr.docsmcp.model.DocsCatalog;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.Disposable;
import reactor.test.StepVerifier;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the wire protocol of both transports the server speaks:
 * Streamable HTTP at {@code /mcp} (what current clients use) and the deprecated
 * HTTP+SSE pair at {@code /sse} + {@code /mcp/message} (older installs).
 *
 * The regression this guards against is the whole reason for the Spring AI 2.0 upgrade:
 * a POST to the MCP endpoint used to return 405 Method Not Allowed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // Replace the ONNX embedding model with a deterministic stub — no 90 MB download in CI.
        "spring.main.allow-bean-definition-overriding=true",
        // Nothing may reach the network: no docs fetch, no query log, no trial webhook.
        "docs.url=file:///jobrunr-docs-mcp-test/no-such-docs.json",
        "docs.manifest-url=file:///jobrunr-docs-mcp-test/no-such-manifest.json",
        "docs.poll-interval=PT24H",
        "log.api.url=",
        "trial.webhook-url=http://127.0.0.1:1/none",
        "ratelimit.enabled=false",
        // Prove the keep-alive fires without spending 25 s of build time waiting for it.
        "mcp.legacy-sse.keep-alive-interval=1s",
        "spring.ai.mcp.server.streamable-http.keep-alive-interval=1s",
})
class McpTransportIntegrationTest {

    private static final String PROTOCOL = "2025-11-25";

    private static final String INITIALIZE = """
            {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
              "protocolVersion":"%s","capabilities":{},
              "clientInfo":{"name":"integration-test","version":"1.0"}}}
            """.formatted(PROTOCOL);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @LocalServerPort
    private int port;

    private WebTestClient client;

    @Autowired
    private DocsRegistry registry;

    @Autowired
    private LuceneIndex luceneIndex;

    @Autowired
    private VectorIndex vectorIndex;

    /** The real loader is pointed at a missing file, so seed the indexes from the bundled sample. */
    @BeforeEach
    void setUp() throws Exception {
        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(60))
                .build();

        if (!registry.pages().isEmpty()) return;
        try (InputStream in = getClass().getResourceAsStream("/sample-docs.json")) {
            DocsCatalog catalog = MAPPER.readValue(in, DocsCatalog.class);
            registry.set(catalog);
            luceneIndex.rebuild(catalog);
            vectorIndex.rebuild(catalog);
        }
    }

    // ---------------------------------------------------------------- streamable

    @Test
    void postToMcpEndpointInitializes() {
        JsonNode result = rpc(initialize()).path("result");

        assertThat(result.path("protocolVersion").stringValue()).isEqualTo(PROTOCOL);
        assertThat(result.path("serverInfo").path("name").stringValue()).isEqualTo("jobrunr-docs");
        assertThat(result.path("capabilities").has("tools")).isTrue();
        assertThat(result.path("instructions").stringValue()).contains("JobRunr documentation MCP server");
    }

    @Test
    void streamableTransportListsAllTools() {
        assertThat(toolNamesOverStreamableHttp()).containsExactlyInAnyOrder(
                "search_jobrunr_docs", "fetch_jobrunr_doc",
                "list_jobrunr_doc_sections", "request_jobrunr_pro_trial");
    }

    @Test
    void streamableTransportExecutesToolCall() {
        String session = openSession();
        JsonNode result = rpc(post(session, """
                {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
                  "name":"search_jobrunr_docs","arguments":{"query":"recurring jobs","limit":3}}}
                """)).path("result");

        assertThat(result.path("isError").booleanValue()).isFalse();
        JsonNode payload = MAPPER.readTree(result.path("content").get(0).path("text").stringValue());
        assertThat(payload.path("results")).isNotEmpty();
        assertThat(payload.path("results").get(0).path("path").stringValue()).contains("recurring-jobs");
    }

    @Test
    void unknownDocPathIsAToolErrorNotATransportError() {
        String session = openSession();
        JsonNode result = rpc(post(session, """
                {"jsonrpc":"2.0","id":3,"method":"tools/call","params":{
                  "name":"fetch_jobrunr_doc","arguments":{"path":"does-not-exist"}}}
                """)).path("result");

        assertThat(result.path("isError").booleanValue()).isTrue();
    }

    @Test
    void clientThatCannotAcceptEventStreamIsRejected() {
        client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .header("MCP-Protocol-Version", PROTOCOL)
                .bodyValue(INITIALIZE)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void clientWithoutProtocolVersionHeaderStillWorks() {
        client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange()
                .expectStatus().isOk();
    }

    // ------------------------------------------------------------------- legacy

    @Test
    void legacySseEndpointStillAdvertisesItsMessageEndpoint() {
        List<String> events = client.get().uri("/sse")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(String.class)
                .getResponseBody()
                .take(1)
                .collectList()
                .block(Duration.ofSeconds(20));

        assertThat(events).isNotNull().hasSize(1);
        assertThat(events.get(0)).contains("/mcp/message").contains("sessionId=");
    }

    @Test
    void legacySseStreamStaysOpenAndPings() {
        // keep-alive replaced the hand-rolled SseKeepAliveFilter; the SDK pings instead.
        StepVerifier.create(client.get().uri("/sse")
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .exchange()
                        .returnResult(String.class)
                        .getResponseBody()
                        .take(2))
                .expectNextMatches(e -> e.contains("/mcp/message"))
                .expectNextMatches(e -> e.contains("\"method\":\"ping\""))
                .expectComplete()
                .verify(Duration.ofSeconds(45));
    }

    @Test
    void legacyTransportCompletesAFullHandshakeAndExposesTheSameTools() throws Exception {
        // A legacy client holds GET /sse open for the whole session and POSTs alongside it;
        // dropping the stream drops the session, so keep the subscription alive throughout.
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        Disposable subscription = client.get().uri("/sse")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(String.class)
                .getResponseBody()
                .subscribe(events::add);

        try {
            String endpoint = events.poll(20, TimeUnit.SECONDS);
            assertThat(endpoint).as("endpoint event").isNotNull().contains("/mcp/message");
            String messageUri = endpoint.substring(endpoint.indexOf("/mcp/message"));

            postToLegacy(messageUri, INITIALIZE);
            JsonNode initResult = MAPPER.readTree(nextNonPing(events)).path("result");
            assertThat(initResult.path("serverInfo").path("name").stringValue()).isEqualTo("jobrunr-docs");

            postToLegacy(messageUri, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
            postToLegacy(messageUri, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}");

            JsonNode tools = MAPPER.readTree(nextNonPing(events)).path("result").path("tools");
            List<String> legacyNames = tools.valueStream().map(t -> t.path("name").stringValue()).toList();

            assertThat(legacyNames).containsExactlyInAnyOrderElementsOf(toolNamesOverStreamableHttp());
            assertThat(legacyNames).containsExactlyInAnyOrder(
                    "search_jobrunr_docs", "fetch_jobrunr_doc",
                    "list_jobrunr_doc_sections", "request_jobrunr_pro_trial");
        } finally {
            subscription.dispose();
        }
    }

    private void postToLegacy(String messageUri, String body) {
        client.post().uri(messageUri)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .exchange()
                .expectStatus().is2xxSuccessful();
    }

    /** Keep-alive pings share the stream with responses; skip them. */
    private static String nextNonPing(BlockingQueue<String> events) throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            String event = events.poll(30, TimeUnit.SECONDS);
            assertThat(event).as("expected a JSON-RPC message on the SSE stream").isNotNull();
            if (!event.contains("\"method\":\"ping\"")) return event;
        }
        throw new AssertionError("only pings arrived on the SSE stream");
    }

    // -------------------------------------------------------------------- helpers

    private List<String> toolNamesOverStreamableHttp() {
        String session = openSession();
        JsonNode tools = rpc(post(session, """
                {"jsonrpc":"2.0","id":9,"method":"tools/list","params":{}}
                """)).path("result").path("tools");
        return tools.valueStream().map(t -> t.path("name").stringValue()).toList();
    }

    private EntityExchangeResult<byte[]> initialize() {
        return client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
                .bodyValue(INITIALIZE)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .returnResult();
    }

    private String openSession() {
        EntityExchangeResult<byte[]> result = initialize();
        String session = result.getResponseHeaders().getFirst("Mcp-Session-Id");
        assertThat(session).as("server must assign a session id").isNotBlank();
        client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
                .header("Mcp-Session-Id", session)
                .bodyValue("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}")
                .exchange()
                .expectStatus().is2xxSuccessful();
        return session;
    }

    private EntityExchangeResult<byte[]> post(String session, String body) {
        return client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
                .header("Mcp-Session-Id", session)
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .returnResult();
    }

    /** The endpoint answers with either a JSON object or a one-message SSE stream; accept both. */
    private static JsonNode rpc(EntityExchangeResult<byte[]> result) {
        String body = new String(result.getResponseBodyContent());
        for (String line : body.split("\n")) {
            if (line.startsWith("data:")) {
                return MAPPER.readTree(line.substring(5).trim());
            }
        }
        return MAPPER.readTree(body);
    }

    @TestConfiguration
    static class StubEmbeddingConfig {
        @Bean
        EmbeddingModel embeddingModel() {
            return new DeterministicEmbeddingModel();
        }
    }
}
