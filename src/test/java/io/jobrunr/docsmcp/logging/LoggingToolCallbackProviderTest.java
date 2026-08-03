package io.jobrunr.docsmcp.logging;

import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.definition.ToolDefinition;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LoggingToolCallbackProviderTest {

    private List<QueryLogEntry> captured;
    private RecordingQueryLogClient recordingClient;

    @BeforeEach
    void setup() {
        captured = new ArrayList<>();
        recordingClient = new RecordingQueryLogClient(captured);
    }

    @AfterEach
    void clearContext() {
        RequestContextHolder.clear();
    }

    @Test
    void wrapsCallbackAndLogsSuccess() {
        ToolCallback stub = new StubToolCallback("search_jobrunr_docs",
                "{\"results\":[{\"path\":\"a\",\"title\":\"A\"}]}");
        LoggingToolCallbackProvider provider = newProvider(stub);

        RequestContextHolder.set(new RequestContextHolder.RequestContext("1.2.3.4", "test-agent/1.0"));

        ToolCallback wrapped = provider.getToolCallbacks()[0];
        String out = wrapped.call("{\"query\":\"recurring jobs\",\"limit\":5}");

        assertThat(out).contains("results");
        assertThat(captured).hasSize(1);
        QueryLogEntry entry = captured.get(0);
        assertThat(entry.toolName()).isEqualTo("search_jobrunr_docs");
        assertThat(entry.success()).isTrue();
        assertThat(entry.errorMessage()).isNull();
        assertThat(entry.clientIp()).isEqualTo("1.2.3.4");
        assertThat(entry.userAgent()).isEqualTo("test-agent/1.0");
        assertThat(entry.arguments()).containsEntry("query", "recurring jobs");
        assertThat(entry.resultMetadata()).containsKey("results");
        assertThat(entry.resultBodyTotalBytes()).isEqualTo(out.length());
        assertThat(entry.latencyMs()).isGreaterThanOrEqualTo(0);
        assertThat(entry.occurredAt()).isNotNull();
        assertThat(entry.mcpServerVersion()).isEqualTo("test-1.0");
    }

    @Test
    void truncatesLargeBody() {
        StringBuilder big = new StringBuilder("{\"x\":\"");
        for (int i = 0; i < 5000; i++) big.append('a');
        big.append("\"}");
        ToolCallback stub = new StubToolCallback("fetch_jobrunr_doc", big.toString());
        LoggingToolCallbackProvider provider = newProvider(stub);

        provider.getToolCallbacks()[0].call("{}");

        QueryLogEntry entry = captured.get(0);
        assertThat(entry.resultBodyTruncated().length()).isEqualTo(64);
        assertThat(entry.resultBodyTotalBytes()).isEqualTo(big.length());
    }

    @Test
    void redactsTrialEmailToDomain() {
        ToolCallback stub = new StubToolCallback("request_jobrunr_pro_trial",
                "{\"success\":true,\"message\":\"ok\"}");
        LoggingToolCallbackProvider provider = newProvider(stub);

        provider.getToolCallbacks()[0]
                .call("{\"email\":\"jane@acme.com\",\"company\":\"Acme\"}");

        QueryLogEntry entry = captured.get(0);
        assertThat(entry.arguments()).doesNotContainKey("email");
        assertThat(entry.arguments()).containsEntry("email_domain", "acme.com");
        assertThat(entry.arguments()).containsEntry("company", "Acme");
    }

    @Test
    void logsAndRethrowsOnException() {
        ToolCallback stub = new StubToolCallback("fetch_jobrunr_doc", null) {
            @Override
            public String call(String input) {
                throw new IllegalArgumentException("Unknown doc path: foo");
            }
        };
        LoggingToolCallbackProvider provider = newProvider(stub);

        assertThatThrownBy(() -> provider.getToolCallbacks()[0].call("{\"path\":\"foo\"}"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(captured).hasSize(1);
        QueryLogEntry entry = captured.get(0);
        assertThat(entry.success()).isFalse();
        assertThat(entry.errorMessage()).contains("Unknown doc path");
    }

    @Test
    void loggingFailureDoesNotBreakToolCall() {
        ToolCallback stub = new StubToolCallback("list_jobrunr_doc_sections", "[]");
        QueryLogClient explodingClient = new QueryLogClient("", "", java.time.Duration.ofSeconds(1)) {
            @Override
            public void fireAndForget(QueryLogEntry entry) {
                throw new RuntimeException("logging blew up");
            }
        };
        LoggingToolCallbackProvider provider = new LoggingToolCallbackProvider(
                ToolCallbackProvider.from(stub), explodingClient, new ObjectMapper(), 64, "test-1.0");

        String out = provider.getToolCallbacks()[0].call("{}");
        assertThat(out).isEqualTo("[]");
    }

    @Test
    void callWithToolContextAlsoLogs() {
        ToolCallback stub = new StubToolCallback("list_jobrunr_doc_sections", "[]");
        LoggingToolCallbackProvider provider = newProvider(stub);

        provider.getToolCallbacks()[0].call("{}", new ToolContext(java.util.Map.of()));
        assertThat(captured).hasSize(1);
        assertThat(captured.get(0).toolName()).isEqualTo("list_jobrunr_doc_sections");
    }

    private LoggingToolCallbackProvider newProvider(ToolCallback... callbacks) {
        return new LoggingToolCallbackProvider(
                ToolCallbackProvider.from(callbacks), recordingClient, new ObjectMapper(), 64, "test-1.0");
    }

    /**
     * Subclass that captures into a list instead of POSTing. We pass blank url/secret to the parent so
     * the parent's enabled flag is false — guarantees no real HTTP attempt even if the override is missed.
     */
    static class RecordingQueryLogClient extends QueryLogClient {
        private final List<QueryLogEntry> sink;

        RecordingQueryLogClient(List<QueryLogEntry> sink) {
            super("", "", java.time.Duration.ofSeconds(1));
            this.sink = sink;
        }

        @Override
        public void fireAndForget(QueryLogEntry entry) {
            sink.add(entry);
        }
    }

    static class StubToolCallback implements ToolCallback {
        private final String name;
        private final String result;

        StubToolCallback(String name, String result) {
            this.name = name;
            this.result = result;
        }

        @Override
        public ToolDefinition getToolDefinition() {
            return ToolDefinition.builder().name(name).description(name).inputSchema("{}").build();
        }

        @Override
        public String call(String input) {
            return result;
        }
    }
}
