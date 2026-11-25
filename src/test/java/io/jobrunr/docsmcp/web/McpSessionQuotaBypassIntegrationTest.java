package io.jobrunr.docsmcp.web;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "docs.url=file:///jobrunr-docs-mcp-test/no-such-docs.json",
        "docs.manifest-url=file:///jobrunr-docs-mcp-test/no-such-manifest.json",
        "docs.poll-interval=PT24H",
        "log.api.url=",
        "trial.webhook-url=",
        "ratelimit.enabled=false",
        "mcp.sessions.max=2",
        "mcp.sessions.max-per-account=1",
        "mcp.sessions.idle-timeout=PT30M"
})
class McpSessionQuotaBypassIntegrationTest {

    private static final String INITIALIZE = """
            {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
              "protocolVersion":"2025-11-25","capabilities":{},
              "clientInfo":{"name":"quota-bypass-test","version":"1.0"}}}
            """;

    @LocalServerPort
    private int port;

    @Autowired
    private AccountStore store;

    @Test
    void unsupportedMethodCannotReleaseTheQuotaForALiveSession() {
        Account account = store.create("quota-bypass-" + System.nanoTime() + "@example.test", "Quota", "Example", null, null);
        String token = store.issueTokens(account.id(), false).agentToken();
        WebTestClient client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .defaultHeader("Authorization", "Bearer " + token)
                .responseTimeout(Duration.ofSeconds(20))
                .build();

        String session = client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isOk().expectBody().returnResult()
                .getResponseHeaders().getFirst("Mcp-Session-Id");
        assertThat(session).isNotBlank();

        client.method(HttpMethod.PUT).uri("/mcp")
                .header("Mcp-Session-Id", session)
                .exchange().expectStatus().isNotFound();
        client.post().uri("/mcp")
                .header("Mcp-Session-Id", session)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}")
                .exchange().expectStatus().isOk();
        client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isEqualTo(429);

        client.delete().uri("/mcp").header("Mcp-Session-Id", session)
                .exchange().expectStatus().is2xxSuccessful();
        String replacement = client.post().uri("/mcp")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue(INITIALIZE)
                .exchange().expectStatus().isOk().expectBody().returnResult()
                .getResponseHeaders().getFirst("Mcp-Session-Id");
        assertThat(replacement).isNotBlank();
        client.delete().uri("/mcp").header("Mcp-Session-Id", replacement)
                .exchange().expectStatus().is2xxSuccessful();
    }

    @TestConfiguration
    static class StubEmbeddingConfig {
        @Bean
        EmbeddingModel embeddingModel() {
            return new DeterministicEmbeddingModel();
        }
    }
}
