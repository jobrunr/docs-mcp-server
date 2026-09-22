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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a small snapshot per connected cluster (version, servers, job counts) for the account page and for sales, and
 * enforces the free tier: one cluster per account at a time. The data comes from the dashboard API through the
 * connector, so the connector itself stays a dumb relay.
 */
@Component
public class InstanceMonitor {

    static final String SECOND_CLUSTER_MESSAGE = "This account already has another JobRunr cluster connected. The free JobRunr MCP "
            + "connects one cluster per account at a time; connecting several clusters is part of JobRunr Pro (https://www.jobrunr.io/en/pro/).";

    private static final Logger log = LoggerFactory.getLogger(InstanceMonitor.class);
    private static final List<String> COUNTED_STATES = List.of("SCHEDULED", "ENQUEUED", "PROCESSING", "FAILED", "SUCCEEDED");

    private static final Duration ON_DEMAND_REFRESH_INTERVAL = Duration.ofSeconds(30);
    private static final Duration LIVENESS_TIMEOUT = Duration.ofSeconds(5);

    private final Map<String, Instant> lastOnDemandRefresh = new ConcurrentHashMap<>();
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
        Mono.fromRunnable(() -> refresh(node))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(null, e -> log.warn("Snapshot of node {} failed: {}", node.nodeId(), e.toString()));
    }

    /** Keeps the account page current while someone looks at it, without polling clusters nobody watches. */
    public void refreshSoon(String accountId) {
        Instant now = Instant.now();
        Instant previous = lastOnDemandRefresh.get(accountId);
        if (previous != null && previous.isAfter(now.minus(ON_DEMAND_REFRESH_INTERVAL))) return;
        lastOnDemandRefresh.put(accountId, now);
        hub.onlineNodes(accountId).stream().findFirst().ifPresent(this::onNodeConnected);
    }

    @Scheduled(fixedDelayString = "${hub.snapshot-interval:PT10M}", initialDelayString = "${hub.snapshot-interval:PT10M}")
    public void refreshAll() {
        hub.onlineNodesOfAllAccounts().forEach(node -> {
            try {
                refresh(node);
            } catch (Exception e) {
                log.warn("Snapshot of node {} failed: {}", node.nodeId(), e.toString());
            }
        });
    }

    void refresh(ConnectorHub.Node node) {
        Account account = store.findById(node.accountId()).orElse(null);
        if (account == null) return;

        JsonNode clusterMetadata = get(node, "/api/metadata/id/cluster");
        String clusterId = clusterMetadata != null && clusterMetadata.path("value").isString() ? clusterMetadata.path("value").stringValue() : null;
        node.clusterId(clusterId);

        if (clusterId != null && !clusterId.equals(account.clusterId())) {
            boolean linkedClusterOnline = account.clusterId() != null && hub.onlineNodes(account.id()).stream()
                    .filter(other -> other != node && account.clusterId().equals(other.clusterId()))
                    .anyMatch(this::answers);
            if (linkedClusterOnline) {
                node.reject(SECOND_CLUSTER_MESSAGE);
                leadEvents.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster " + clusterId);
                return;
            }
            // the linked cluster is gone (new database, new environment): the account moves to this one
            store.linkCluster(account.id(), clusterId);
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

        if (first || !store.hasLeadEvent(account.id(), LeadEvents.FIRST_CONNECTION)) {
            leadEvents.record(account, LeadEvents.FIRST_CONNECTION, "JobRunr " + info.jobrunrVersion() + ", " + info.framework()
                    + ", " + (servers != null ? servers.size() : 0) + " servers, " + total(recurringJobs) + " recurring jobs");
        }
    }

    /** A node can look connected for a moment after its JVM stopped; only a round trip proves it is still there. */
    private boolean answers(ConnectorHub.Node node) {
        return Boolean.TRUE.equals(hub.sendTo(node, "GET", "/api/version", LIVENESS_TIMEOUT)
                .map(response -> response.status() < 500)
                .onErrorReturn(false)
                .block());
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
