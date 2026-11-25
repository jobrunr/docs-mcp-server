package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final Duration ACTIVE_WINDOW = Duration.ofSeconds(30);
    private static final Duration FORGET_AFTER = Duration.ofMinutes(5);
    private static final int OSS_NODE_LIMIT = 2;
    private static final int MAX_NEW_NODE_REGISTRATIONS_PER_MINUTE = 120;
    private static final int MAX_POLL_LINES = 64;
    private static final int MAX_PENDING_PER_ACCOUNT = 256;
    private static final int MAX_PENDING_GLOBAL = 10_000;
    private static final int MAX_REQUEST_LINE_LENGTH = 768;

    private record NodeKey(String accountId, String nodeId) {}

    private final Map<NodeKey, Node> nodes = new ConcurrentHashMap<>();
    private final Map<String, PendingRequest> pending = new ConcurrentHashMap<>();
    /** Updated with pending under the same lock to avoid scanning all requests on every enqueue. */
    private final Map<String, Integer> pendingPerAccount = new HashMap<>();
    /** New IDs only; repeat polls of an existing JVM are never throttled. Guarded by nodes. */
    private final Map<String, Deque<Instant>> newNodeRegistrations = new HashMap<>();
    private final Duration pollHold;
    private final Duration requestTimeout;
    private final int maxNodesGlobal;

    public ConnectorHub(HubProperties properties, @Value("${hub.max-nodes-global:10000}") int maxNodesGlobal) {
        if (maxNodesGlobal < OSS_NODE_LIMIT) throw new IllegalArgumentException("hub.max-nodes-global must be at least two");
        this.pollHold = properties.pollHold();
        this.requestTimeout = properties.requestTimeout();
        this.maxNodesGlobal = maxNodesGlobal;
    }

    public record NodeInfo(String jobrunrVersion, String javaVersion, String framework, boolean redaction, boolean canOperate) {
        public NodeInfo(String jobrunrVersion, String javaVersion, String framework, boolean redaction) {
            this(jobrunrVersion, javaVersion, framework, redaction, true);
        }
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
        private final AtomicInteger inFlight = new AtomicInteger();
        private volatile NodeInfo info;
        private volatile Instant lastPollAt = Instant.now();
        private volatile Instant lastSnapshotAttemptAt;
        private volatile String clusterId;
        private volatile boolean ready;
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

        private NodeKey key() {
            return new NodeKey(accountId, nodeId);
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

        public Instant lastSnapshotAttemptAt() {
            return lastSnapshotAttemptAt;
        }

        public void markSnapshotAttempt() {
            this.lastSnapshotAttemptAt = Instant.now();
        }

        public String clusterId() {
            return clusterId;
        }

        public void clusterId(String clusterId) {
            this.clusterId = clusterId;
        }

        /** Traffic is routed only after the monitor verified the node's cluster membership. */
        public boolean isReady() {
            return ready && rejectedReason == null;
        }

        public void markReady() {
            this.ready = true;
        }

        public String rejectedReason() {
            return rejectedReason;
        }

        public void reject(String reason) {
            this.rejectedReason = reason;
            this.ready = false;
        }

        synchronized boolean isPolling() {
            return waiter != null;
        }

        /** A held poll or outstanding request keeps a slow connector live through the result timeout. */
        public boolean isActive() {
            return rejectedReason == null &&
                    (isPolling() || inFlight.get() > 0 || lastPollAt.isAfter(Instant.now().minus(ACTIVE_WINDOW)));
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
            List<String> lines = new ArrayList<>(Math.min(queue.size(), MAX_POLL_LINES));
            while (!queue.isEmpty() && lines.size() < MAX_POLL_LINES) lines.add(queue.removeFirst());
            return lines;
        }

        private synchronized Sinks.Empty<Void> newWaiter() {
            if (waiter != null) return null;
            waiter = Sinks.empty();
            if (!queue.isEmpty()) waiter.tryEmitEmpty();
            return waiter;
        }

        private synchronized boolean clearWaiter(Sinks.Empty<Void> done) {
            if (waiter != done) return false;
            waiter = null;
            return true;
        }
    }

    private record PendingRequest(String accountId, String requestLine, Node node, Sinks.One<TunnelResponse> sink) {
    }

    // ------------------------------------------------------------------ connector side

    public Registration register(String accountId, String nodeId, NodeInfo info) {
        return register(accountId, nodeId, info, false);
    }

    /** Enforces the OSS live-node quota atomically. Pro accounts have no per-account node quota. */
    public Registration register(String accountId, String nodeId, NodeInfo info, boolean proEnabled) {
        synchronized (nodes) {
            Instant now = Instant.now();
            NodeKey key = new NodeKey(accountId, nodeId);
            Node node = nodes.get(key);
            if (node != null) {
                node.info = info;
                node.lastPollAt = now;
                return new Registration(node, false);
            }
            // A stopped JVM must not occupy one of the two OSS slots for the five-minute retention window.
            nodes.entrySet().removeIf(entry -> entry.getKey().accountId().equals(accountId) && !entry.getValue().isActive());
            if (!proEnabled && nodes.keySet().stream().filter(k -> k.accountId().equals(accountId)).count() >= OSS_NODE_LIMIT) {
                throw new NodeLimitException();
            }
            if (nodes.size() >= maxNodesGlobal) {
                // Reclaim stopped nodes before denying another customer a slot.
                nodes.values().removeIf(existing -> !existing.isActive());
                if (nodes.size() >= maxNodesGlobal) throw new GlobalNodeLimitException();
            }
            Deque<Instant> recent = newNodeRegistrations.computeIfAbsent(accountId, ignored -> new ArrayDeque<>());
            Instant cutoff = now.minus(Duration.ofMinutes(1));
            while (!recent.isEmpty() && !recent.peekFirst().isAfter(cutoff)) recent.removeFirst();
            if (recent.size() >= MAX_NEW_NODE_REGISTRATIONS_PER_MINUTE) throw new NodeRegistrationRateException();
            recent.addLast(now);
            node = new Node(nodeId, accountId);
            node.info = info;
            nodes.put(key, node);
            return new Registration(node, true);
        }
    }

    /** Hands the node its queued requests, holding the poll open for a while if there are none. */
    public Mono<List<String>> awaitRequests(Node node) {
        return Mono.defer(() -> {
            Sinks.Empty<Void> waiter = node.newWaiter();
            if (waiter == null) return Mono.error(new AlreadyPollingException());
            return waiter.asMono()
                    .timeout(pollHold, Mono.empty())
                    .then(Mono.fromSupplier(node::drain))
                    .doFinally(signal -> {
                        if (node.clearWaiter(waiter)) {
                            if (signal == SignalType.CANCEL) {
                                // the connector hung up mid-poll: that JVM stopped, forget it right away
                                node.lastPollAt = Instant.EPOCH;
                                nodes.remove(node.key(), node);
                            } else {
                                node.lastPollAt = Instant.now();
                            }
                        }
                    });
        });
    }

    public void forget(Node node) {
        nodes.remove(node.key(), node);
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
        pending.values().stream()
                .filter(request -> request.accountId.equals(accountId))
                .forEach(request -> request.sink.tryEmitError(new NoInstanceConnectedException()));
    }

    public boolean complete(String accountId, String nodeId, String requestId, int status, String body) {
        PendingRequest request = pending.get(requestId);
        if (request == null || !request.accountId.equals(accountId) || !request.node.nodeId.equals(nodeId)) return false;
        return request.sink.tryEmitValue(new TunnelResponse(status, body)).isSuccess();
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

    /** Routes only to the selected, verified cluster of this account; an offline cluster never falls back. */
    public Mono<TunnelResponse> send(String accountId, String linkedClusterId, String method, String path) {
        return Mono.defer(() -> {
            List<Node> online = onlineNodes(accountId).stream().filter(Node::isReady).toList();
            List<Node> clusterNodes = online.stream()
                    .filter(node -> linkedClusterId == null || linkedClusterId.equals(node.clusterId))
                    .toList();
            if (clusterNodes.isEmpty()) {
                if (linkedClusterId != null) return Mono.error(new ClusterNotConnectedException(linkedClusterId));
                return Mono.error(new NoInstanceConnectedException());
            }
            List<Node> eligible = clusterNodes.stream()
                    .filter(node -> "GET".equals(method) || node.info.canOperate())
                    .toList();
            Node target = eligible.stream()
                    .min(Comparator.comparing(node -> !node.isPolling()))
                    .orElse(null);
            if (target == null) return Mono.error(new ReadOnlyConnectorException());
            return sendTo(target, method, path);
        });
    }

    public Mono<TunnelResponse> sendTo(Node node, String method, String path) {
        return sendTo(node, method, path, requestTimeout);
    }

    public Mono<TunnelResponse> sendTo(Node node, String method, String path, Duration timeout) {
        return Mono.defer(() -> {
            if (node.rejectedReason != null) return Mono.error(new NoInstanceConnectedException());
            if (!"GET".equals(method) && !node.info.canOperate()) return Mono.error(new ReadOnlyConnectorException());
            String requestId = UUID.randomUUID().toString().replace("-", "");
            String requestLine = requestId + " " + method + " " + path;
            if (requestLine.length() > MAX_REQUEST_LINE_LENGTH || requestLine.indexOf('\n') >= 0 || requestLine.indexOf('\r') >= 0) {
                return Mono.error(new IllegalArgumentException("Invalid or oversized dashboard request"));
            }
            PendingRequest request = new PendingRequest(node.accountId, requestLine, node, Sinks.one());
            synchronized (pending) {
                if (pending.size() >= MAX_PENDING_GLOBAL ||
                        pendingPerAccount.getOrDefault(node.accountId, 0) >= MAX_PENDING_PER_ACCOUNT) {
                    return Mono.error(new TooManyPendingRequestsException());
                }
                pending.put(requestId, request);
                pendingPerAccount.merge(node.accountId, 1, Integer::sum);
                node.inFlight.incrementAndGet();
            }
            node.enqueue(requestLine);
            return request.sink.asMono()
                    .timeout(timeout)
                    .onErrorMap(TimeoutException.class, e -> new InstanceTimeoutException(timeout))
                    .doFinally(signal -> {
                        synchronized (pending) {
                            if (pending.remove(requestId, request)) {
                                pendingPerAccount.computeIfPresent(node.accountId, (key, count) -> count == 1 ? null : count - 1);
                                node.inFlight.decrementAndGet();
                            }
                        }
                        // a request that was never picked up must not run later, after the agent was told it failed
                        node.unqueue(requestLine);
                    });
        });
    }

    @Scheduled(fixedDelay = 60_000)
    public void forgetDisconnectedNodes() {
        Instant threshold = Instant.now().minus(FORGET_AFTER);
        synchronized (nodes) {
            nodes.values().removeIf(node -> !node.isPolling() && node.inFlight.get() == 0 &&
                    node.lastPollAt.isBefore(threshold));
            Instant registrationCutoff = Instant.now().minus(Duration.ofMinutes(1));
            newNodeRegistrations.values().removeIf(recent -> {
                while (!recent.isEmpty() && !recent.peekFirst().isAfter(registrationCutoff)) recent.removeFirst();
                return recent.isEmpty();
            });
        }
    }

    public static class NoInstanceConnectedException extends RuntimeException {
        public NoInstanceConnectedException() {
            super("No JobRunr instance is connected to this account right now.");
        }
    }

    public static class ClusterNotConnectedException extends RuntimeException {
        public ClusterNotConnectedException(String clusterId) {
            super("Cluster " + clusterId + " is not connected to this account.");
        }
    }

    public static class NodeLimitException extends RuntimeException {
        public NodeLimitException() {
            super("The free JobRunr MCP plan allows at most two live nodes in one cluster. " +
                    "JobRunr Pro allows unlimited connected nodes and clusters: https://www.jobrunr.io/en/pro/");
        }
    }

    public static class NodeRegistrationRateException extends RuntimeException {
        public NodeRegistrationRateException() {
            super("Too many new connector node IDs were registered in one minute; retry shortly.");
        }
    }

    public static class GlobalNodeLimitException extends RuntimeException {
        public GlobalNodeLimitException() {
            super("The hub is at its connector capacity; retry shortly.");
        }
    }

    public static class AlreadyPollingException extends RuntimeException {
        public AlreadyPollingException() {
            super("This connector node already has a poll in progress.");
        }
    }

    public static class ReadOnlyConnectorException extends RuntimeException {
        public ReadOnlyConnectorException() {
            super("The connected JobRunr instance has a read-only token and does not allow this operation.");
        }
    }

    public static class TooManyPendingRequestsException extends RuntimeException {
        public TooManyPendingRequestsException() {
            super("Too many dashboard requests are pending for this account.");
        }
    }

    public static class InstanceTimeoutException extends RuntimeException {
        public InstanceTimeoutException(Duration timeout) {
            super("The connected JobRunr instance did not answer within " + timeout.toSeconds() + " seconds.");
        }
    }
}
