package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Keeps a small snapshot per connected cluster (version, servers, job counts) for the account page and for sales, and
 * enforces the free tier: one cluster per account at a time. The data comes from the dashboard API through the
 * connector, so the connector itself stays a dumb relay.
 */
@Component
public class InstanceMonitor {

    static final String SECOND_CLUSTER_MESSAGE = "This account already has another JobRunr cluster connected. The free JobRunr MCP "
            + "connects one cluster with up to two nodes; JobRunr Pro allows unlimited connected nodes and clusters "
            + "(https://www.jobrunr.io/en/pro/).";

    private static final Logger log = LoggerFactory.getLogger(InstanceMonitor.class);
    private static final List<String> COUNTED_STATES = List.of("SCHEDULED", "ENQUEUED", "PROCESSING", "FAILED", "SUCCEEDED");

    private static final Duration ON_DEMAND_REFRESH_INTERVAL = Duration.ofSeconds(30);
    private static final int MAX_CONCURRENT_SNAPSHOTS = 8;
    private static final int MAX_QUEUED_SNAPSHOTS = 512;
    private static final int MAX_ON_DEMAND_REFRESH_ACCOUNTS = 10_000;
    private static final Duration FAILED_SNAPSHOT_RETRY = Duration.ofSeconds(15);
    private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private final Map<String, Instant> lastOnDemandRefresh = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
            return size() > MAX_ON_DEMAND_REFRESH_ACCOUNTS;
        }
    };
    private final Semaphore snapshotSlots = new Semaphore(MAX_CONCURRENT_SNAPSHOTS);
    private final Deque<ConnectorHub.Node> snapshotQueue = new ArrayDeque<>();
    private final Set<ConnectorHub.Node> queuedSnapshots = new HashSet<>();
    /** Serializes cluster claims per account without retaining attacker-controlled lock keys. */
    private final Object[] clusterLocks = IntStream.range(0, 256).mapToObj(i -> new Object()).toArray();
    private final ConnectorHub hub;
    private final AccountStore store;
    private final LeadEvents leadEvents;
    private final ObjectMapper objectMapper;

    public InstanceMonitor(ConnectorHub hub, AccountStore store, LeadEvents leadEvents, ObjectMapper objectMapper) {
        this.hub = hub;
        this.store = store;
        this.leadEvents = leadEvents;
        this.objectMapper = objectMapper;
    }

    public void onNodeConnected(ConnectorHub.Node node) {
        synchronized (snapshotQueue) {
            if (queuedSnapshots.contains(node)) return;
            if (snapshotQueue.size() >= MAX_QUEUED_SNAPSHOTS) {
                log.debug("Snapshot queue full; node {} will be retried", node.nodeId());
                return;
            }
            queuedSnapshots.add(node);
            snapshotQueue.addLast(node);
        }
        drainSnapshotQueue();
    }

    private void drainSnapshotQueue() {
        while (snapshotSlots.tryAcquire()) {
            ConnectorHub.Node next;
            synchronized (snapshotQueue) {
                next = snapshotQueue.pollFirst();
            }
            if (next == null) {
                snapshotSlots.release();
                return;
            }
            if (!next.isActive()) {
                synchronized (snapshotQueue) {
                    queuedSnapshots.remove(next);
                }
                snapshotSlots.release();
                continue;
            }
            next.markSnapshotAttempt();
            Mono.fromRunnable(() -> refresh(next))
                    .subscribeOn(Schedulers.boundedElastic())
                    .doFinally(signal -> {
                        synchronized (snapshotQueue) {
                            queuedSnapshots.remove(next);
                        }
                        snapshotSlots.release();
                        drainSnapshotQueue();
                    })
                    .subscribe(null, e -> log.warn("Snapshot of node {} failed: {}", next.nodeId(), e.toString()));
        }
    }

    @Scheduled(fixedDelay = 5_000)
    public void retryUnreadyNodes() {
        Instant cutoff = Instant.now().minus(FAILED_SNAPSHOT_RETRY);
        hub.onlineNodesOfAllAccounts().stream()
                .filter(node -> !node.isReady())
                .filter(node -> node.lastSnapshotAttemptAt() == null || node.lastSnapshotAttemptAt().isBefore(cutoff))
                .forEach(this::onNodeConnected);
    }

    /** Keeps the account page current while someone looks at it, without polling clusters nobody watches. */
    public void refreshSoon(String accountId) {
        Instant now = Instant.now();
        synchronized (lastOnDemandRefresh) {
            Instant previous = lastOnDemandRefresh.get(accountId);
            if (previous != null && previous.isAfter(now.minus(ON_DEMAND_REFRESH_INTERVAL))) return;
            lastOnDemandRefresh.put(accountId, now);
        }
        Set<String> clusters = new HashSet<>();
        hub.onlineNodes(accountId).stream()
                .filter(ConnectorHub.Node::isReady)
                .filter(node -> clusters.add(node.clusterId()))
                .forEach(this::onNodeConnected);
    }

    @Scheduled(fixedDelayString = "${hub.snapshot-interval:PT10M}", initialDelayString = "${hub.snapshot-interval:PT10M}")
    public void refreshAll() {
        Set<String> clusters = new HashSet<>();
        hub.onlineNodesOfAllAccounts().stream()
                .filter(node -> !node.isReady() || clusters.add(node.accountId() + "\u0000" + node.clusterId()))
                .forEach(this::onNodeConnected);
        synchronized (lastOnDemandRefresh) {
            lastOnDemandRefresh.entrySet().removeIf(entry -> entry.getValue().isBefore(Instant.now().minus(Duration.ofMinutes(2))));
        }
    }

    void refresh(ConnectorHub.Node node) {
        Account account = store.findById(node.accountId()).orElse(null);
        if (account == null) return;

        JsonNode clusterMetadata = get(node, "/api/metadata/id/cluster");
        // The connector can start before the BackgroundJobServer writes its cluster metadata.
        // Keep the node unready and let retryUnreadyNodes check again; rejecting it here makes
        // the connector's next poll fail permanently with 409 after an ordinary startup race.
        if (clusterMetadata == null) return;
        String clusterId = clusterMetadata.path("value").isString() ? clusterMetadata.path("value").stringValue() : null;
        if (clusterId == null || !CLUSTER_ID.matcher(clusterId).matches()) {
            node.reject("The connector did not return a valid JobRunr cluster id.");
            return;
        }
        node.clusterId(clusterId);

        synchronized (clusterLock(account.id())) {
            // Re-read inside the account lock: two first nodes can finish the metadata call together.
            account = store.findById(node.accountId()).orElse(null);
            if (account == null) return;
            if (!account.proEnabled() && node.isReady() && !clusterId.equals(account.clusterId())) {
                // A Pro downgrade must not silently adopt a previously connected extra cluster.
                rejectSecondCluster(account, node, clusterId);
                return;
            }
            if (!clusterId.equals(account.clusterId())) {
                if (account.proEnabled()) {
                    // The first Pro cluster is the default; further Pro clusters are selected explicitly by id.
                    if (account.clusterId() == null) store.claimClusterIfUnlinked(account.id(), clusterId);
                } else if (account.clusterId() == null) {
                    if (!store.claimClusterIfUnlinked(account.id(), clusterId) && !isLinkedTo(account.id(), clusterId)) {
                        rejectSecondCluster(account, node, clusterId);
                        return;
                    }
                } else {
                    String linkedId = account.clusterId();
                    // A Pro account that used more than one cluster keeps its original
                    // default pinned on downgrade. Ordinary OSS history has no fence,
                    // so A -> B -> A remains possible.
                    if (store.hadProMultiCluster(account.id())) {
                        rejectSecondCluster(account, node, clusterId);
                        return;
                    }
                    boolean linkedClusterOnline = hub.onlineNodes(account.id()).stream()
                            .filter(other -> other != node && linkedId.equals(other.clusterId()))
                            .findAny().isPresent();
                    if (linkedClusterOnline) {
                        rejectSecondCluster(account, node, clusterId);
                        return;
                    }
                    // A stopped OSS cluster may be replaced, but only one contender can claim the slot.
                    if (!store.moveClusterIfCurrent(account.id(), account.clusterId(), clusterId) &&
                            !isLinkedTo(account.id(), clusterId)) {
                        rejectSecondCluster(account, node, clusterId);
                        return;
                    }
                }
            }
            if (!account.proEnabled() && !isLinkedTo(account.id(), clusterId)) {
                rejectSecondCluster(account, node, clusterId);
                return;
            }
            if (account.proEnabled() && !isLinkedTo(account.id(), clusterId)) {
                // Persist the downgrade fence before an extra Pro node becomes
                // routable. Failed stats snapshots cannot bypass it.
                store.markProMultiClusterSeen(account.id());
            }
            // Registration alone is insufficient: route traffic only after cluster identity and
            // account membership have been checked under the same lock.
            node.markReady();
        }

        JsonNode version = get(node, "/api/version");
        JsonNode servers = get(node, "/api/servers");
        JsonNode recurringJobs = get(node, "/api/recurring-jobs?offset=0&limit=1");
        Long[] counts = COUNTED_STATES.stream().map(state -> total(get(node, "/api/jobs?state=" + state + "&offset=0&limit=1"))).toArray(Long[]::new);

        ConnectorHub.NodeInfo info = node.info();
        String storageProvider = version != null && version.path("storageProviderType").isString() ? version.path("storageProviderType").stringValue() : null;
        boolean first = store.saveInstance(new AccountStore.InstanceSnapshot(
                account.id(), clusterId, info.jobrunrVersion(), info.javaVersion(), info.framework(), storageProvider, info.redaction(),
                servers != null && servers.isArray() ? servers.size() : null, total(recurringJobs),
                counts[0], counts[1], counts[2], counts[3], counts[4], null, Instant.now()));

        if (first) {
            Set<String> connectedClusters = Set.copyOf(hub.onlineNodes(account.id()).stream()
                    .filter(ConnectorHub.Node::isReady).map(ConnectorHub.Node::clusterId).toList());
            store.pruneDisconnectedSnapshots(account.id(), connectedClusters);
        }

        if (first || !store.hasLeadEvent(account.id(), LeadEvents.FIRST_CONNECTION)) {
            leadEvents.record(clusterId != null ? account.withClusterId(clusterId) : account, LeadEvents.FIRST_CONNECTION, "JobRunr " + info.jobrunrVersion() + ", " + info.framework()
                    + ", " + (servers != null ? servers.size() : 0) + " servers, " + total(recurringJobs) + " recurring jobs");
        }
    }

    private Object clusterLock(String accountId) {
        return clusterLocks[Math.floorMod(accountId.hashCode(), clusterLocks.length)];
    }

    private boolean isLinkedTo(String accountId, String clusterId) {
        return store.findById(accountId).map(account -> clusterId.equals(account.clusterId())).orElse(false);
    }

    private void rejectSecondCluster(Account account, ConnectorHub.Node node, String clusterId) {
        node.reject(SECOND_CLUSTER_MESSAGE);
        leadEvents.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster " + clusterId);
    }

    private JsonNode get(ConnectorHub.Node node, String path) {
        ConnectorHub.TunnelResponse response = hub.sendTo(node, "GET", path).block(Duration.ofSeconds(60));
        if (response == null || response.status() != 200 || response.body() == null || response.body().isBlank()) return null;
        return objectMapper.readTree(response.body());
    }

    private static Long total(JsonNode page) {
        return page != null && page.path("total").isNumber() ? page.path("total").longValue() : null;
    }
}
