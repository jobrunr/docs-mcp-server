package io.jobrunr.docsmcp.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.definition.ToolDefinition;
import org.springframework.ai.tool.metadata.ToolMetadata;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class LoggingToolCallbackProvider implements ToolCallbackProvider {

    private static final Logger log = LoggerFactory.getLogger(LoggingToolCallbackProvider.class);

    private static final int MAX_FIELD_CHARS = 256;
    private static final int MAX_ERROR_CHARS = 500;

    private final ToolCallbackProvider delegate;
    private final QueryLogClient queryLogClient;
    private final ObjectMapper objectMapper;
    private final int bodyTruncateBytes;
    private final String serverVersion;

    public LoggingToolCallbackProvider(ToolCallbackProvider delegate,
                                       QueryLogClient queryLogClient,
                                       ObjectMapper objectMapper,
                                       int bodyTruncateBytes,
                                       String serverVersion) {
        this.delegate = delegate;
        this.queryLogClient = queryLogClient;
        this.objectMapper = objectMapper;
        this.bodyTruncateBytes = bodyTruncateBytes;
        this.serverVersion = serverVersion;
    }

    @Override
    public ToolCallback[] getToolCallbacks() {
        ToolCallback[] underlying = delegate.getToolCallbacks();
        return Arrays.stream(underlying)
                .map(cb -> (ToolCallback) new LoggingToolCallback(cb))
                .toArray(ToolCallback[]::new);
    }

    private final class LoggingToolCallback implements ToolCallback {

        private final ToolCallback delegate;

        LoggingToolCallback(ToolCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public ToolDefinition getToolDefinition() {
            return delegate.getToolDefinition();
        }

        @Override
        public ToolMetadata getToolMetadata() {
            return delegate.getToolMetadata();
        }

        @Override
        public String call(String input) {
            return invokeAndLog(input, null, false);
        }

        @Override
        public String call(String input, ToolContext context) {
            return invokeAndLog(input, context, true);
        }

        private String invokeAndLog(String input, ToolContext ctx, boolean withContext) {
            String toolName = delegate.getToolDefinition().name();
            Instant occurredAt = Instant.now();
            long t0 = System.nanoTime();
            boolean success = true;
            String result = null;
            String errorMessage = null;
            RuntimeException toRethrow = null;

            try {
                result = withContext ? delegate.call(input, ctx) : delegate.call(input);
                return result;
            } catch (RuntimeException ex) {
                success = false;
                errorMessage = truncate(ex.getMessage() != null ? ex.getMessage() : ex.toString(), MAX_ERROR_CHARS);
                toRethrow = ex;
                throw ex;
            } finally {
                long latencyMs = (System.nanoTime() - t0) / 1_000_000L;
                try {
                    QueryLogEntry entry = buildEntry(toolName, input, result, success, errorMessage, latencyMs, occurredAt);
                    queryLogClient.fireAndForget(entry);
                } catch (Throwable t) {
                    log.warn("Failed to enqueue MCP query log entry: {}", t.toString());
                }
                // toRethrow is implicit via try/catch — we just ensure the finally never swallows it
                if (toRethrow != null && Thread.interrupted()) Thread.currentThread().interrupt();
            }
        }
    }

    private QueryLogEntry buildEntry(String toolName, String input, String result, boolean success,
                                     String errorMessage, long latencyMs, Instant occurredAt) {
        Map<String, Object> args = parseArguments(toolName, input);
        Map<String, Object> metadata = null;
        String truncatedBody = null;
        Integer totalBytes = null;
        if (result != null) {
            metadata = extractMetadata(result);
            totalBytes = result.length();
            truncatedBody = result.length() <= bodyTruncateBytes ? result : result.substring(0, bodyTruncateBytes);
        }
        RequestContextHolder.RequestContext rc = RequestContextHolder.peek();
        String clientIp = rc != null ? rc.clientIp() : null;
        String userAgent = rc != null ? rc.userAgent() : null;

        return new QueryLogEntry(toolName, args, metadata, truncatedBody, totalBytes,
                success, errorMessage, latencyMs, clientIp, userAgent, serverVersion, occurredAt);
    }

    Map<String, Object> parseArguments(String toolName, String input) {
        if (input == null || input.isBlank()) return null;
        Map<String, Object> args;
        try {
            args = objectMapper.readValue(input, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            Map<String, Object> raw = new LinkedHashMap<>();
            raw.put("_raw", truncate(input, MAX_FIELD_CHARS));
            return raw;
        }
        if ("request_jobrunr_pro_trial".equals(toolName)) {
            redactTrialArgs(args);
        }
        return args;
    }

    private static void redactTrialArgs(Map<String, Object> args) {
        Object email = args.remove("email");
        if (email instanceof String s) {
            int at = s.indexOf('@');
            args.put("email_domain", at >= 0 && at < s.length() - 1 ? s.substring(at + 1) : null);
        }
        Object useCase = args.get("useCase");
        if (useCase instanceof String s && s.length() > 200) {
            args.put("useCase", s.substring(0, 200));
        }
    }

    Map<String, Object> extractMetadata(String resultJson) {
        try {
            JsonNode node = objectMapper.readTree(resultJson);
            JsonNode trimmed = trim(node);
            return objectMapper.convertValue(trimmed, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return null;
        }
    }

    private JsonNode trim(JsonNode node) {
        if (node.isString()) {
            String s = node.stringValue();
            if (s.length() > MAX_FIELD_CHARS) {
                return objectMapper.getNodeFactory().stringNode("<truncated:" + s.length() + ">");
            }
            return node;
        }
        if (node.isArray()) {
            var arr = objectMapper.createArrayNode();
            node.forEach(child -> arr.add(trim(child)));
            return arr;
        }
        if (node.isObject()) {
            ObjectNode obj = objectMapper.createObjectNode();
            node.properties().forEach(e -> obj.set(e.getKey(), trim(e.getValue())));
            return obj;
        }
        return node;
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
