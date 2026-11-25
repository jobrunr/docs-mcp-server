package io.jobrunr.docsmcp.web;

import io.jobrunr.docsmcp.index.DocsRegistry;
import io.jobrunr.docsmcp.index.LuceneIndex;
import io.jobrunr.docsmcp.index.VectorIndex;
import io.jobrunr.docsmcp.hub.mcp.OpsMcpServerConfig;
import io.jobrunr.docsmcp.model.DocsCatalog;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.springframework.ai.mcp.server.webflux.transport.WebFluxStatelessServerTransport;
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
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the Streamable HTTP wire protocol at {@code /mcp}.
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
        "trial.webhook-url=",
        "ratelimit.enabled=false",
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

    @Autowired
    private WebFluxStatelessServerTransport publicTransport;

    @Autowired
    private OpsMcpServerConfig.OpsMcpEndpoint opsEndpoint;

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
        initialize();
        JsonNode result = rpc(post("""
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
        initialize();
        JsonNode result = rpc(post("""
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

    @Test
    void publicDocsDoNotIssueSessionIds() {
        assertThat(publicTransport).isNotNull();
        assertThat(initialize().getResponseHeaders().getFirst("Mcp-Session-Id")).isNull();
        assertThat(initialize().getResponseHeaders().getFirst("Mcp-Session-Id")).isNull();
    }

    @Test
    void authenticatedOpsTransportHasSessionCapAndIdleExpiry() {
        Object opsTransport = ReflectionTestUtils.getField(opsEndpoint, "provider");
        assertThat(ReflectionTestUtils.getField(opsTransport, "maxSessions")).isEqualTo(2000L);
        assertThat(ReflectionTestUtils.getField(opsTransport, "sessionIdleTimeout")).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void oldSseEndpointsAreGone() {
        client.get().uri("/sse").exchange().expectStatus().isNotFound();
        client.post().uri("/mcp/message?sessionId=test")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{}")
                .exchange()
                .expectStatus().isNotFound();
    }

    // -------------------------------------------------------------------- helpers

    private List<String> toolNamesOverStreamableHttp() {
        initialize();
        JsonNode tools = rpc(post("""
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

    private EntityExchangeResult<byte[]> post(String body) {
        return client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
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
