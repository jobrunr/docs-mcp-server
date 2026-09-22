package io.jobrunr.docsmcp.hub;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.account.Mailer;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole OSS flow against a fake connector: signup, email link, tokens, connector long-poll, and MCP tool calls
 * with an agent token that the hub relays to the "customer JVM".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "docs.url=file:///jobrunr-docs-mcp-test/no-such-docs.json",
        "docs.manifest-url=file:///jobrunr-docs-mcp-test/no-such-manifest.json",
        "docs.poll-interval=PT24H",
        "log.api.url=",
        "trial.webhook-url=http://127.0.0.1:1/none",
        "ratelimit.enabled=false",
        "hub.poll-hold=PT1S",
        "hub.request-timeout=PT10S",
        "hub.public-url=http://localhost",
})
class OpsHubIntegrationTest {

    private static final String PROTOCOL = "2025-11-25";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String FAILED_JOB_1 = "0199aaaa-0000-7000-8000-000000000001";
    private static final String FAILED_JOB_2 = "0199aaaa-0000-7000-8000-000000000002";

    @LocalServerPort
    private int port;

    @Autowired
    private CapturingMailer mailer;

    private WebTestClient client;
    private FakeConnector connector;

    @BeforeEach
    void setUp() {
        client = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).responseTimeout(Duration.ofSeconds(60)).build();
    }

    @AfterEach
    void tearDown() {
        if (connector != null) connector.stop();
    }

    @Test
    void ossUserSignsUpConnectsTheirClusterAndOperatesItThroughAnAgent() {
        // sign up and follow the emailed link
        String email = "dev" + System.nanoTime() + "@acme.io";
        client.post().uri("/api/signup").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("name", "Jane", "email", email, "company", "Acme", "useCase", "requeue failed invoices"))
                .exchange().expectStatus().isOk();
        String link = mailer.lastLinkFor(email);
        assertThat(link).startsWith("http://localhost/verify#");

        String sessionCookie = client.post().uri("/api/verify").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("token", link.substring(link.indexOf('#') + 1)))
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseCookies().getFirst("jr_mcp_session").getValue();

        // the link works once
        client.post().uri("/api/verify").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("token", link.substring(link.indexOf('#') + 1)))
                .exchange().expectStatus().isEqualTo(410);

        JsonNode tokens = MAPPER.readTree(client.post().uri("/api/account/tokens").cookie("jr_mcp_session", sessionCookie)
                .contentType(MediaType.APPLICATION_JSON).bodyValue(Map.of("operate", true))
                .exchange().expectStatus().isOk().expectBody(String.class).returnResult().getResponseBody());
        String connectorToken = tokens.path("connectorToken").stringValue();
        String agentToken = tokens.path("agentToken").stringValue();
        assertThat(connectorToken).startsWith("jrc_operate_");
        assertThat(agentToken).startsWith("jra_");

        // the customer's application connects
        connector = new FakeConnector("http://localhost:" + port, connectorToken);
        connector.start();
        JsonNode account = eventually(() -> accountPage(sessionCookie), page -> page.path("instance").path("jobsFailed").asLong() == 2);
        assertThat(account.path("nodes").get(0).path("jobrunrVersion").stringValue()).isEqualTo("9.0.0");
        assertThat(account.path("instance").path("clusterId").stringValue()).isEqualTo("cluster-1");

        // the agent sees docs and cluster tools
        String session = openSession(agentToken);
        List<String> tools = rpc(post(agentToken, session, "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}"))
                .path("result").path("tools").valueStream().map(t -> t.path("name").stringValue()).toList();
        assertThat(tools).contains("search_jobrunr_docs", "request_jobrunr_pro_trial", "get_cluster_overview", "triage_failed_jobs", "requeue_jobs");

        JsonNode overview = callTool(agentToken, session, "get_cluster_overview", Map.of());
        assertThat(overview.path("jobrunrVersion").stringValue()).isEqualTo("9.0.0");
        assertThat(overview.path("jobCounts").path("failed").asLong()).isEqualTo(2);
        assertThat(overview.path("backgroundJobServers")).hasSize(1);

        JsonNode triage = callTool(agentToken, session, "triage_failed_jobs", Map.of());
        assertThat(triage.path("groups")).hasSize(1);
        assertThat(triage.path("groups").get(0).path("count").asInt()).isEqualTo(2);
        assertThat(triage.path("groups").get(0).path("exceptionType").stringValue()).isEqualTo("java.net.SocketTimeoutException");

        JsonNode job = callTool(agentToken, session, "get_job", Map.of("job_id", FAILED_JOB_1));
        assertThat(job.path("state").stringValue()).isEqualTo("FAILED");

        // bulk requeue: preview first, then execute exactly that preview
        List<String> ids = List.of(FAILED_JOB_1, FAILED_JOB_2);
        JsonNode preview = callTool(agentToken, session, "requeue_jobs", Map.of("job_ids", ids));
        assertThat(preview.path("willRequeue").asInt()).isEqualTo(2);
        assertThat(connector.requeued).isEmpty();

        JsonNode withoutPreview = callToolRaw(agentToken, session, "requeue_jobs", Map.of("job_ids", ids, "dry_run", false));
        assertThat(withoutPreview.path("isError").booleanValue()).isTrue();

        JsonNode executed = callTool(agentToken, session, "requeue_jobs",
                Map.of("job_ids", ids, "dry_run", false, "preview_id", preview.path("previewId").stringValue()));
        assertThat(executed.path("requeued").asInt()).isEqualTo(2);
        assertThat(connector.requeued).containsExactlyInAnyOrder(FAILED_JOB_1, FAILED_JOB_2);

        // a Pro feature answers with a hint instead of failing
        JsonNode search = callTool(agentToken, session, "search_jobs", Map.of("query", "invoice"));
        assertThat(search.path("proFeature").booleanValue()).isTrue();

        // usage shows up on the account page, without arguments or results
        JsonNode activity = accountPage(sessionCookie).path("recentToolCalls");
        assertThat(activity.valueStream().map(c -> c.path("toolName").stringValue()).toList()).contains("get_cluster_overview", "requeue_jobs");
    }

    @Test
    void unknownAgentTokenIsRejectedAndTheOpsEndpointIsNotReachableDirectly() {
        client.post().uri("/mcp").header("Authorization", "Bearer jra_doesnotexist")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue("{}").exchange().expectStatus().isUnauthorized();
        client.post().uri("/mcp-ops")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .bodyValue("{}").exchange().expectStatus().isNotFound();
    }

    @Test
    void connectorWithUnknownTokenIsRejected() {
        client.get().uri("/connector/poll").header("Authorization", "Bearer jrc_read_doesnotexist").header("X-JobRunr-Node", "node-12345678")
                .exchange().expectStatus().isUnauthorized();
    }

    @Test
    void agentWithoutConnectedInstanceGetsSetupInstructions(@Autowired AccountStore store) {
        Account account = store.create("lonely" + System.nanoTime() + "@acme.io", "Lonely", "Acme", null, null);
        String agentToken = store.issueTokens(account.id(), false).agentToken();

        JsonNode result = callToolRaw(agentToken, openSession(agentToken), "get_cluster_overview", Map.of());
        assertThat(result.path("isError").booleanValue()).isTrue();
        assertThat(result.path("content").get(0).path("text").stringValue()).contains("jobrunr.dashboard.mcp.token");
    }

    // -------------------------------------------------------------------- helpers

    private JsonNode accountPage(String sessionCookie) {
        return MAPPER.readTree(client.get().uri("/api/account").cookie("jr_mcp_session", sessionCookie)
                .exchange().expectStatus().isOk().expectBody(String.class).returnResult().getResponseBody());
    }

    private JsonNode callTool(String agentToken, String session, String tool, Map<String, Object> arguments) {
        JsonNode result = callToolRaw(agentToken, session, tool, arguments);
        assertThat(result.path("isError").booleanValue()).as(tool + " failed: " + result).isFalse();
        return MAPPER.readTree(result.path("content").get(0).path("text").stringValue());
    }

    private JsonNode callToolRaw(String agentToken, String session, String tool, Map<String, Object> arguments) {
        String body = MAPPER.writeValueAsString(Map.of("jsonrpc", "2.0", "id", 3, "method", "tools/call",
                "params", Map.of("name", tool, "arguments", arguments)));
        return rpc(post(agentToken, session, body)).path("result");
    }

    private String openSession(String agentToken) {
        EntityExchangeResult<byte[]> result = client.post().uri("/mcp")
                .header("Authorization", "Bearer " + agentToken)
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
                .bodyValue("""
                        {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"%s","capabilities":{},
                         "clientInfo":{"name":"integration-test","version":"1.0"}}}""".formatted(PROTOCOL))
                .exchange().expectStatus().isOk().expectBody().returnResult();
        assertThat(rpc(result).path("result").path("serverInfo").path("name").stringValue()).isEqualTo("jobrunr");
        String session = result.getResponseHeaders().getFirst("Mcp-Session-Id");
        post(agentToken, session, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
        return session;
    }

    private EntityExchangeResult<byte[]> post(String agentToken, String session, String body) {
        return client.post().uri("/mcp")
                .header("Authorization", "Bearer " + agentToken)
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON, MediaType.TEXT_EVENT_STREAM)
                .header("MCP-Protocol-Version", PROTOCOL)
                .header("Mcp-Session-Id", session)
                .bodyValue(body)
                .exchange().expectStatus().is2xxSuccessful().expectBody().returnResult();
    }

    private static JsonNode rpc(EntityExchangeResult<byte[]> result) {
        byte[] content = result.getResponseBodyContent();
        String body = content == null ? "" : new String(content);
        for (String line : body.split("\n")) {
            if (line.startsWith("data:")) return MAPPER.readTree(line.substring(5).trim());
        }
        return body.isBlank() ? MAPPER.nullNode() : MAPPER.readTree(body);
    }

    private static JsonNode eventually(Supplier<JsonNode> supplier, java.util.function.Predicate<JsonNode> condition) {
        long deadline = System.currentTimeMillis() + 20_000;
        JsonNode last = null;
        while (System.currentTimeMillis() < deadline) {
            last = supplier.get();
            if (condition.test(last)) return last;
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new AssertionError("Condition not met, last value: " + last);
    }

    /** Plays the connector inside a customer's JVM, answering with canned dashboard API responses. */
    static class FakeConnector {
        final Set<String> requeued = ConcurrentHashMap.newKeySet();
        private final HttpClient http = HttpClient.newHttpClient();
        private final String hubUrl;
        private final String token;
        private volatile boolean running = true;
        private Thread thread;

        FakeConnector(String hubUrl, String token) {
            this.hubUrl = hubUrl;
            this.token = token;
        }

        void start() {
            thread = Thread.ofVirtual().start(() -> {
                while (running) {
                    try {
                        HttpResponse<String> poll = http.send(HttpRequest.newBuilder(URI.create(hubUrl + "/connector/poll"))
                                .header("Authorization", "Bearer " + token)
                                .header("X-JobRunr-Node", "test-node-0001")
                                .header("X-JobRunr-Version", "9.0.0")
                                .header("X-JobRunr-Java-Version", "21")
                                .header("X-JobRunr-Framework", "spring-boot")
                                .header("X-JobRunr-Redaction", "true")
                                .GET().build(), HttpResponse.BodyHandlers.ofString());
                        if (poll.statusCode() != 200) continue;
                        for (String line : poll.body().split("\n")) {
                            if (line.isBlank()) continue;
                            String[] parts = line.split(" ");
                            Thread.ofVirtual().start(() -> answer(parts[0], parts[1], parts[2]));
                        }
                    } catch (Exception e) {
                        if (!running) return;
                    }
                }
            });
        }

        void stop() {
            running = false;
            thread.interrupt();
        }

        private void answer(String id, String method, String path) {
            String[] response = respond(method, path);
            try {
                http.send(HttpRequest.newBuilder(URI.create(hubUrl + "/connector/results/" + id))
                        .header("Authorization", "Bearer " + token)
                        .header("X-JobRunr-Status", response[0])
                        .POST(HttpRequest.BodyPublishers.ofString(response[1])).build(), HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {
                // the test fails on the missing answer
            }
        }

        private String[] respond(String method, String path) {
            if (method.equals("POST") && path.endsWith("/requeue")) {
                requeued.add(path.split("/")[3]);
                return new String[]{"204", ""};
            }
            if (path.equals("/api/version")) return ok("{\"version\":\"9.0.0\",\"allowAnonymousDataUsage\":false}");
            if (path.equals("/api/metadata/id/cluster")) return ok("{\"name\":\"id\",\"owner\":\"cluster\",\"value\":\"cluster-1\"}");
            if (path.equals("/api/servers")) return ok("[{\"id\":\"3f1c\",\"name\":\"worker-1\",\"workerPoolSize\":8,\"running\":true}]");
            if (path.equals("/api/problems")) return ok("[]");
            if (path.startsWith("/api/recurring-jobs")) return ok("{\"total\":1,\"items\":[{\"id\":\"daily-report\",\"jobName\":\"Daily report\",\"scheduleExpression\":\"0 2 * * *\"}]}");
            if (path.startsWith("/api/jobs?state=FAILED")) return ok("{\"total\":2,\"items\":[" + failedJob(FAILED_JOB_1) + "," + failedJob(FAILED_JOB_2) + "]}");
            if (path.startsWith("/api/jobs?state=")) return ok("{\"total\":0,\"items\":[]}");
            if (path.equals("/api/jobs/" + FAILED_JOB_1)) return ok(failedJob(FAILED_JOB_1));
            if (path.equals("/api/jobs/" + FAILED_JOB_2)) return ok(failedJob(FAILED_JOB_2));
            return new String[]{"404", ""};
        }

        private static String[] ok(String body) {
            return new String[]{"200", body};
        }

        private static String failedJob(String id) {
            return """
                    {"id":"%s","jobName":"Send invoice","jobSignature":"org.acme.InvoiceService.send(java.lang.Long)","labels":[],
                     "jobDetails":{"className":"org.acme.InvoiceService","methodName":"send","jobParameters":[{"className":"java.lang.Long","actualClassName":"java.lang.Long","object":"<redacted>"}]},
                     "jobHistory":[{"state":"ENQUEUED","createdAt":"2026-09-22T09:00:00Z"},{"state":"PROCESSING","createdAt":"2026-09-22T09:00:01Z","serverName":"worker-1"},
                       {"state":"FAILED","createdAt":"2026-09-22T09:00:31Z","exceptionType":"java.net.SocketTimeoutException","exceptionMessage":"Read timed out after %d ms","stackTrace":"java.net.SocketTimeoutException: Read timed out"}]}"""
                    .formatted(id, id.endsWith("1") ? 30000 : 30001);
        }
    }

    static class CapturingMailer extends Mailer {
        private final Map<String, String> links = new ConcurrentHashMap<>();

        CapturingMailer(ObjectProvider<JavaMailSender> mailSender, HubProperties properties) {
            super(mailSender, properties);
        }

        @Override
        public void sendSignInLink(Account account, String link, boolean firstTime) {
            links.put(account.email(), link);
        }

        String lastLinkFor(String email) {
            return links.get(email);
        }
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        EmbeddingModel embeddingModel() {
            return new DeterministicEmbeddingModel();
        }

        @Bean
        @Primary
        CapturingMailer capturingMailer(ObjectProvider<JavaMailSender> mailSender, HubProperties properties) {
            return new CapturingMailer(mailSender, properties);
        }
    }
}
