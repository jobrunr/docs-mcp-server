package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Relays dashboard API requests to the connectors running inside customer JVMs.
 * <p>
 * Every JVM with a connector is a {@link Node}. A node long-polls {@code /connector/poll}; requests for it wait in its
 * queue until it picks them up, and its answer arrives on {@code /connector/results/{id}}. State is in memory, so the
 * hub runs as a single instance.
 */
@Component
public class ConnectorHub {

    /** A live connector is back in a poll within milliseconds; a node silent for longer than this is gone. */
    private static final Duration ACTIVE_WINDOW = Duration.ofSeconds(5);
    private static final Duration FORGET_AFTER = Duration.ofMinutes(5);

    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final Map<String, PendingRequest> pending = new ConcurrentHashMap<>();
    private final Duration pollHold;
    private final Duration requestTimeout;

    public ConnectorHub(HubProperties properties) {
        this.pollHold = properties.pollHold();
        this.requestTimeout = properties.requestTimeout();
    }

    public record NodeInfo(String jobrunrVersion, String javaVersion, String framework, boolean redaction) {
    }

    public record TunnelResponse(int status, String body) {
    }

    public record Registration(Node node, boolean isNew) {
    }

    public static final class Node {
        private final String nodeId;
        private final String accountId;
        private final Instant connectedAt = Instant.now();
        private final Deque<String> queue = new ArrayDeque<>();
        private volatile NodeInfo info;
        private volatile Instant lastPollAt = Instant.now();
        private volatile String clusterId;
        private volatile String rejectedReason;
        private Sinks.Empty<Void> waiter; // guarded by this

        private Node(String nodeId, String accountId) {
            this.nodeId = nodeId;
            this.accountId = accountId;
        }

        public String nodeId() {
            return nodeId;
        }

        public String accountId() {
            return accountId;
        }

        public NodeInfo info() {
            return info;
        }

        public Instant connectedAt() {
            return connectedAt;
        }

        public Instant lastPollAt() {
            return lastPollAt;
        }

        public String clusterId() {
            return clusterId;
        }

        public void clusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        public String rejectedReason() {
            return rejectedReason;
        }

        public void reject(String reason) {
            this.rejectedReason = reason;
        }

        synchronized boolean isPolling() {
            return waiter != null;
        }

        /** Holding a long-poll right now, or just finished one: the connector in that JVM is alive. */
        public boolean isActive() {
            return rejectedReason == null && (isPolling() || lastPollAt.isAfter(Instant.now().minus(ACTIVE_WINDOW)));
        }

        void markPolledAt(Instant lastPollAt) {
            this.lastPollAt = lastPollAt;
        }

        private synchronized void enqueue(String requestLine) {
            queue.add(requestLine);
            if (waiter != null) waiter.tryEmitEmpty();
        }

        private synchronized void unqueue(String requestLine) {
            queue.remove(requestLine);
        }

        private synchronized List<String> drain() {
            List<String> lines = new ArrayList<>(queue);
            queue.clear();
            return lines;
        }

        private synchronized Sinks.Empty<Void> newWaiter() {
            waiter = Sinks.empty();
            if (!queue.isEmpty()) waiter.tryEmitEmpty();
            return waiter;
        }

        private synchronized void clearWaiter(Sinks.Empty<Void> done) {
            if (waiter == done) waiter = null;
        }
    }

    private record PendingRequest(String accountId, String requestLine, Node node, Sinks.One<TunnelResponse> sink) {
    }

    // ------------------------------------------------------------------ connector side

    public Registration register(String accountId, String nodeId, NodeInfo info) {
        boolean[] isNew = {false};
        Node node = nodes.compute(nodeId, (id, existing) -> {
            if (existing != null && existing.accountId.equals(accountId)) return existing;
            isNew[0] = true;
            return new Node(id, accountId);
        });
        node.info = info;
        node.lastPollAt = Instant.now();
        return new Registration(node, isNew[0]);
    }

    /** Hands the node its queued requests, holding the poll open for a while if there are none. */
    public Mono<List<String>> awaitRequests(Node node) {
        Sinks.Empty<Void> waiter = node.newWaiter();
        return waiter.asMono()
                .timeout(pollHold, Mono.empty())
                .then(Mono.fromSupplier(node::drain))
                .doFinally(signal -> {
                    node.clearWaiter(waiter);
                    if (signal == SignalType.CANCEL) {
                        // the connector hung up mid-poll: that JVM stopped, forget it right away
                        node.lastPollAt = Instant.EPOCH;
                        nodes.remove(node.nodeId, node);
                    } else {
                        node.lastPollAt = Instant.now();
                    }
                });
    }

    public void forget(Node node) {
        nodes.remove(node.nodeId, node);
    }

    /** Drops every node of the account, for example after its tokens were rotated. Their next poll must authenticate again. */
    public void disconnectAccount(String accountId) {
        nodes.values().removeIf(node -> {
            if (!node.accountId.equals(accountId)) return false;
            node.reject("The tokens of this account were rotated.");
            synchronized (node) {
                if (node.waiter != null) node.waiter.tryEmitEmpty();
            }
            return true;
        });
    }

    public boolean complete(String accountId, String requestId, int status, String body) {
        PendingRequest request = pending.get(requestId);
        if (request == null || !request.accountId.equals(accountId)) return false;
        request.sink.tryEmitValue(new TunnelResponse(status, body));
        return true;
    }

    // ------------------------------------------------------------------ agent side

    public List<Node> onlineNodes(String accountId) {
        return nodes.values().stream()
                .filter(node -> node.accountId.equals(accountId) && node.isActive())
                .sorted(Comparator.comparing(Node::lastPollAt).reversed())
                .toList();
    }

    public List<Node> onlineNodesOfAllAccounts() {
        return nodes.values().stream().filter(Node::isActive).toList();
    }

    /** Sends a dashboard API request to a connected node of the account, preferring nodes of its linked cluster. */
    public Mono<TunnelResponse> send(String accountId, String linkedClusterId, String method, String path) {
        Node target = onlineNodes(accountId).stream()
                .min(Comparator.comparing((Node node) -> !node.isPolling())
                        .thenComparing(node -> linkedClusterId != null && !linkedClusterId.equals(node.clusterId)))
                .orElse(null);
        if (target == null) return Mono.error(new NoInstanceConnectedException());
        return sendTo(target, method, path);
    }

    public Mono<TunnelResponse> sendTo(Node node, String method, String path) {
        return sendTo(node, method, path, requestTimeout);
    }

    public Mono<TunnelResponse> sendTo(Node node, String method, String path, Duration timeout) {
        String requestId = UUID.randomUUID().toString().replace("-", "");
        String requestLine = requestId + " " + method + " " + path;
        PendingRequest request = new PendingRequest(node.accountId, requestLine, node, Sinks.one());
        pending.put(requestId, request);
        node.enqueue(requestLine);
        return request.sink.asMono()
                .timeout(timeout)
                .onErrorMap(TimeoutException.class, e -> new InstanceTimeoutException(timeout))
                .doFinally(signal -> {
                    pending.remove(requestId);
                    // a request that was never picked up must not run later, after the agent was told it failed
                    node.unqueue(requestLine);
                });
    }

    @Scheduled(fixedDelay = 60_000)
    public void forgetDisconnectedNodes() {
        Instant threshold = Instant.now().minus(FORGET_AFTER);
        nodes.values().removeIf(node -> !node.isPolling() && node.lastPollAt.isBefore(threshold));
    }

    public static class NoInstanceConnectedException extends RuntimeException {
        public NoInstanceConnectedException() {
            super("No JobRunr instance is connected to this account right now.");
        }
    }

    public static class InstanceTimeoutException extends RuntimeException {
        public InstanceTimeoutException(Duration timeout) {
            super("The connected JobRunr instance did not answer within " + timeout.toSeconds() + " seconds.");
        }
    }
}
