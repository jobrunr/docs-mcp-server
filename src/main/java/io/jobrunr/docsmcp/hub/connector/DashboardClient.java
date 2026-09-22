package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.account.Account;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Blocking, typed access to the JobRunr dashboard API of a customer's instance, for the MCP tools. Tools run on
 * Reactor's bounded elastic scheduler, so blocking here is fine.
 */
@Component
public class DashboardClient {

    private static final Duration BLOCK_LIMIT = Duration.ofMinutes(2);

    private final ConnectorHub hub;
    private final ObjectMapper objectMapper;

    public DashboardClient(ConnectorHub hub, ObjectMapper objectMapper) {
        this.hub = hub;
        this.objectMapper = objectMapper;
    }

    public JsonNode get(Account account, String path) {
        ConnectorHub.TunnelResponse response = call(account, "GET", path);
        return response.body() == null || response.body().isBlank() ? objectMapper.nullNode() : objectMapper.readTree(response.body());
    }

    /** @return the json body, or null if the dashboard answered 404 */
    public JsonNode getOrNull(Account account, String path) {
        try {
            return get(account, path);
        } catch (DashboardException e) {
            if (e.status() == 404) return null;
            throw e;
        }
    }

    /**
     * Runs the requests in parallel, so the connector picks them up in one poll. A 404 yields null in the result list.
     */
    public List<JsonNode> getAll(Account account, List<String> paths) {
        List<ConnectorHub.TunnelResponse> responses = Flux.mergeSequential(paths.stream()
                        .map(path -> hub.send(account.id(), account.clusterId(), "GET", path))
                        .toList())
                .collectList()
                .block(BLOCK_LIMIT);
        List<JsonNode> result = new ArrayList<>();
        for (ConnectorHub.TunnelResponse response : responses) {
            if (response.status() == 404) result.add(null);
            else if (response.status() >= 400) throw new DashboardException(response.status(), errorMessage(response));
            else result.add(response.body() == null || response.body().isBlank() ? objectMapper.nullNode() : objectMapper.readTree(response.body()));
        }
        return result;
    }

    /** Runs the mutating requests in parallel and returns the error message per path, null when it succeeded. */
    public List<String> executeAll(Account account, String method, List<String> paths) {
        List<ConnectorHub.TunnelResponse> responses = Flux.mergeSequential(paths.stream()
                        .map(path -> hub.send(account.id(), account.clusterId(), method, path)
                                .onErrorResume(e -> Mono.just(new ConnectorHub.TunnelResponse(504, objectMapper.writeValueAsString(Map.of("error", String.valueOf(e.getMessage())))))))
                        .toList())
                .collectList()
                .block(BLOCK_LIMIT);
        return responses.stream().map(response -> response.status() >= 400 ? errorMessage(response) : null).toList();
    }

    public void execute(Account account, String method, String path) {
        call(account, method, path);
    }

    private ConnectorHub.TunnelResponse call(Account account, String method, String path) {
        ConnectorHub.TunnelResponse response = hub.send(account.id(), account.clusterId(), method, path).block(BLOCK_LIMIT);
        if (response == null) throw new DashboardException(504, "No answer from the connected JobRunr instance.");
        if (response.status() >= 400) throw new DashboardException(response.status(), errorMessage(response));
        return response;
    }

    private String errorMessage(ConnectorHub.TunnelResponse response) {
        try {
            JsonNode error = objectMapper.readTree(response.body()).path("error");
            if (error.isString()) return error.stringValue();
        } catch (Exception ignored) {
            // not json, fall through
        }
        return "HTTP " + response.status();
    }

    public static class DashboardException extends RuntimeException {
        private final int status;

        public DashboardException(int status, String message) {
            super(message);
            this.status = status;
        }

        public int status() {
            return status;
        }
    }
}
