package io.jobrunr.docsmcp.web;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.mcp.OpsMcpServerConfig;
import io.jobrunr.docsmcp.hub.mcp.OpsSessionQuotaWebFilter;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.mcp.server.webflux.transport.WebFluxStatelessServerTransport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Public docs have no sessions; authenticated ops sessions retain a small configurable cap and idle expiry. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "docs.url=file:///jobrunr-docs-mcp-test/no-such-docs.json",
        "docs.manifest-url=file:///jobrunr-docs-mcp-test/no-such-manifest.json",
        "docs.poll-interval=PT24H",
        "log.api.url=",
        "trial.webhook-url=",
        "ratelimit.enabled=false",
        "spring.ai.mcp.server.streamable-http.keep-alive-interval=1s",
        "mcp.sessions.max=3",
        "mcp.sessions.max-per-account=2",
        "mcp.sessions.idle-timeout=PT3S"
})
class McpSessionLimitsIntegrationTest {

    private static final String INITIALIZE = """
            {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
              "protocolVersion":"2025-11-25","capabilities":{},
              "clientInfo":{"name":"session-limit-test","version":"1.0"}}}
            """;

    @LocalServerPort
    private int port;

    @Autowired
    private WebFluxStatelessServerTransport publicTransport;

    @Autowired
    private OpsMcpServerConfig.OpsMcpEndpoint opsEndpoint;

    @Autowired
    private OpsSessionQuotaWebFilter sessionQuota;

    @Autowired
    private AccountStore store;

    @Test
    void anonymousInitializeFloodCannotConsumeAnotherClientsSessionCapacity() {
        WebTestClient publicClient = client();
        assertThat(publicTransport).isNotNull();
        for (int i = 0; i < 100; i++) {
            assertThat(initialize(publicClient).getResponseHeaders().getFirst("Mcp-Session-Id")).isNull();
        }
        // An independent docs caller still initializes and lists tools after the anonymous flood.
        assertThat(initialize(publicClient).getResponseHeaders().getFirst("Mcp-Session-Id")).isNull();
        publicClient.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}")
                .exchange().expectStatus().isOk();
    }

    @Test
    void oneAccountsOpsSessionsCannotConsumeAnotherAccountsAllotment() throws Exception {
        Account account = store.create("ops-sessions-" + System.nanoTime() + "@example.test", "Test", "Example", null, null);
        String token = store.issueTokens(account.id(), false).agentToken();
        WebTestClient opsClient = client().mutate().defaultHeader("Authorization", "Bearer " + token).build();
        Account other = store.create("other-sessions-" + System.nanoTime() + "@example.test", "Other", "Example", null, null);
        String otherToken = store.issueTokens(other.id(), false).agentToken();
        WebTestClient otherClient = client().mutate().defaultHeader("Authorization", "Bearer " + otherToken).build();
        Object provider = ReflectionTestUtils.getField(opsEndpoint, "provider");
        @SuppressWarnings("unchecked")
        Map<String, ?> sessions = (Map<String, ?>) ReflectionTestUtils.getField(provider, "sessions");
        assertThat(sessions).isEmpty();

        String first = sessionId(initialize(opsClient));
        String second = sessionId(initialize(opsClient));
        assertThat(sessions).hasSize(2);
        opsClient.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isEqualTo(429);
        String otherSession = sessionId(initialize(otherClient));
        assertThat(sessions).containsKey(otherSession);
        otherClient.post().uri("/mcp")
                .header("Mcp-Session-Id", first)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}")
                .exchange().expectStatus().isNotFound();

        opsClient.delete().uri("/mcp").header("Mcp-Session-Id", first)
                .exchange().expectStatus().is2xxSuccessful();
        assertThat(sessions).doesNotContainKey(first);
        String third = sessionId(initialize(opsClient));
        assertThat(sessions).containsKeys(second, third, otherSession);
        otherClient.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isEqualTo(503);

        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (!sessions.isEmpty() && System.nanoTime() < deadline) Thread.sleep(100);
        assertThat(sessions).as("idle eviction must release abandoned ops sessions").isEmpty();

        // The quota keeps ownership through the SDK's eviction interval to avoid
        // admitting extra sessions while the SDK still retains the old ones.
        @SuppressWarnings("unchecked")
        Map<String, ?> owners = (Map<String, ?>) ReflectionTestUtils.getField(sessionQuota, "owners");
        while (!owners.isEmpty() && System.nanoTime() < deadline) {
            sessionQuota.purgeIdleOwners();
            Thread.sleep(100);
        }
        assertThat(owners).as("the account quota must be released after idle eviction").isEmpty();

        String fourth = sessionId(initialize(opsClient));
        assertThat(sessions).containsKey(fourth);
        opsClient.delete().uri("/mcp").header("Mcp-Session-Id", fourth)
                .exchange().expectStatus().is2xxSuccessful();
    }

    @Test
    void tokenRotationClosesOnlyTheOldAccountsOpsSessions() throws Exception {
        Account account = store.create("rotate-sessions-" + System.nanoTime() + "@example.test", "Rotate", "Example", null, null);
        String oldToken = store.issueTokens(account.id(), false).agentToken();
        WebTestClient oldClient = client().mutate().defaultHeader("Authorization", "Bearer " + oldToken).build();
        Account other = store.create("rotate-other-" + System.nanoTime() + "@example.test", "Other", "Example", null, null);
        String otherToken = store.issueTokens(other.id(), false).agentToken();
        WebTestClient otherClient = client().mutate().defaultHeader("Authorization", "Bearer " + otherToken).build();
        String first = sessionId(initialize(oldClient));
        String second = sessionId(initialize(oldClient));
        String otherSession = sessionId(initialize(otherClient));

        Object provider = ReflectionTestUtils.getField(opsEndpoint, "provider");
        @SuppressWarnings("unchecked")
        Map<String, ?> sessions = (Map<String, ?>) ReflectionTestUtils.getField(provider, "sessions");
        assertThat(sessions).containsKeys(first, second, otherSession);

        String accountCookie = store.createSession(account.id(), Duration.ofHours(1));
        String json = client().post().uri("/api/account/tokens").cookie("jr_mcp_session", accountCookie)
                .header("Origin", "http://localhost:" + port)
                .contentType(MediaType.APPLICATION_JSON).bodyValue(Map.of("operate", false))
                .exchange().expectStatus().isOk().expectBody(String.class).returnResult().getResponseBody();
        String newToken = new ObjectMapper().readTree(json).path("agentToken").stringValue();
        assertThat(sessions).containsKey(otherSession).doesNotContainKeys(first, second);

        oldClient.post().uri("/mcp").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM).bodyValue(INITIALIZE)
                .exchange().expectStatus().isUnauthorized();
        WebTestClient newClient = client().mutate().defaultHeader("Authorization", "Bearer " + newToken).build();
        String replacement = sessionId(initialize(newClient));
        assertThat(sessions).containsKeys(otherSession, replacement);
        newClient.delete().uri("/mcp").header("Mcp-Session-Id", replacement)
                .exchange().expectStatus().is2xxSuccessful();
        otherClient.delete().uri("/mcp").header("Mcp-Session-Id", otherSession)
                .exchange().expectStatus().is2xxSuccessful();
    }

    private WebTestClient client() {
        return WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(20))
                .build();
    }

    private static EntityExchangeResult<byte[]> initialize(WebTestClient client) {
        return client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isOk()
                .expectBody().returnResult();
    }

    private static String sessionId(EntityExchangeResult<byte[]> result) {
        String id = result.getResponseHeaders().getFirst("Mcp-Session-Id");
        assertThat(id).isNotBlank();
        return id;
    }

    @TestConfiguration
    static class StubEmbeddingConfig {
        @Bean
        EmbeddingModel embeddingModel() {
            return new DeterministicEmbeddingModel();
        }
    }
}
