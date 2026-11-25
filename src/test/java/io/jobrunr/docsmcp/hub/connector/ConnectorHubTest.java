package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorHubTest {

    private static final ConnectorHub.NodeInfo INFO = new ConnectorHub.NodeInfo("9.0.0", "21", "spring-boot", true);

    private final ConnectorHub hub = new ConnectorHub(new HubProperties("http://localhost", "hub@test", null,
            Duration.ofSeconds(2), Duration.ofMillis(500), Duration.ofMinutes(30), Duration.ofDays(1)), 10_000);

    @Test
    void aPollingNodeGetsTheRequestAndItsAnswerCompletesTheCall() {
        ConnectorHub.Node node = hub.register("account-1", "node-aaaaaaaa", INFO).node();
        node.clusterId("cluster-a");
        node.markReady();
        List<String>[] handedOut = new List[1];
        Disposable poll = hub.awaitRequests(node).subscribe(lines -> handedOut[0] = lines);

        StepVerifier.create(hub.send("account-1", null, "GET", "/api/servers"))
                .then(() -> {
                    assertThat(handedOut[0]).hasSize(1);
                    String requestId = handedOut[0].getFirst().split(" ")[0];
                    assertThat(handedOut[0].getFirst()).endsWith(" GET /api/servers");
                    assertThat(hub.complete("other-account", "node-aaaaaaaa", requestId, 200, "[]")).isFalse();
                    assertThat(hub.complete("account-1", "node-wrong00", requestId, 200, "[]")).isFalse();
                    assertThat(hub.complete("account-1", "node-aaaaaaaa", requestId, 200, "[]")).isTrue();
                })
                .assertNext(response -> assertThat(response.body()).isEqualTo("[]"))
                .verifyComplete();
        poll.dispose();
    }

    @Test
    void aNodeThatStoppedPollingIsNotOnlineAndDoesNotGetRequests() {
        ConnectorHub.Node dead = hub.register("account-1", "node-dead0000", INFO).node();
        dead.clusterId("old-cluster");
        dead.markReady();
        dead.markPolledAt(Instant.now().minusSeconds(30));
        ConnectorHub.Node alive = hub.register("account-1", "node-alive000", INFO).node();
        alive.clusterId("new-cluster");
        alive.markReady();
        List<String>[] handedOut = new List[1];
        Disposable poll = hub.awaitRequests(alive).subscribe(lines -> handedOut[0] = lines);

        assertThat(hub.onlineNodes("account-1")).containsExactly(alive);
        // A stale default never falls back to another cluster.
        StepVerifier.create(hub.send("account-1", "old-cluster", "GET", "/api/version"))
                .expectError(ConnectorHub.ClusterNotConnectedException.class).verify();
        assertThat(handedOut[0] == null || handedOut[0].isEmpty()).isTrue();
        poll.dispose();
    }

    @Test
    void aConnectorThatHangsUpMidPollIsForgottenImmediately() {
        ConnectorHub.Node node = hub.register("account-1", "node-gone0000", INFO).node();
        Disposable poll = hub.awaitRequests(node).subscribe();
        assertThat(hub.onlineNodes("account-1")).containsExactly(node);

        poll.dispose();

        assertThat(hub.onlineNodes("account-1")).isEmpty();
    }

    @Test
    void aRequestThatTimesOutBeforePickupNeverRunsLater() {
        ConnectorHub.Node node = hub.register("account-1", "node-slow0000", INFO).node();

        StepVerifier.create(hub.sendTo(node, "POST", "/api/jobs/0199aaaa-0000-7000-8000-000000000001/requeue"))
                .expectError(ConnectorHub.InstanceTimeoutException.class)
                .verify(Duration.ofSeconds(5));

        StepVerifier.create(hub.awaitRequests(node))
                .assertNext(lines -> assertThat(lines).isEmpty())
                .verifyComplete();
    }

    @Test
    void rotatingTokensDisconnectsTheNodesOfTheAccount() {
        hub.register("account-1", "node-aaaaaaaa", INFO);
        hub.register("account-2", "node-bbbbbbbb", INFO);

        hub.disconnectAccount("account-1");

        assertThat(hub.onlineNodes("account-1")).isEmpty();
        assertThat(hub.onlineNodes("account-2")).hasSize(1);
        StepVerifier.create(hub.send("account-1", null, "GET", "/api/version"))
                .expectError(ConnectorHub.NoInstanceConnectedException.class)
                .verify();
    }

    @Test
    void ossHasTwoLiveNodeSlotsButAStaleNodeDoesNotHoldItsSlot() {
        ConnectorHub.Node first = hub.register("account-1", "node-aaaaaaaa", INFO, false).node();
        hub.register("account-1", "node-bbbbbbbb", INFO, false);
        assertThatThrownBy(() -> hub.register("account-1", "node-cccccccc", INFO, false))
                .isInstanceOf(ConnectorHub.NodeLimitException.class);

        first.markPolledAt(Instant.now().minusSeconds(30));
        assertThat(hub.register("account-1", "node-cccccccc", INFO, false).isNew()).isTrue();
        assertThat(hub.onlineNodes("account-1")).hasSize(2);
    }

    @Test
    void proCanRegisterMultipleNodesAndStrictSelectionTargetsOnlyAnOwnedLiveCluster() {
        ConnectorHub.Node first = hub.register("account-a", "node-aaaaaaaa", INFO, true).node();
        first.clusterId("cluster-a");
        first.markReady();
        ConnectorHub.Node second = hub.register("account-a", "node-bbbbbbbb", INFO, true).node();
        second.clusterId("cluster-b");
        second.markReady();
        ConnectorHub.Node third = hub.register("account-a", "node-cccccccc", INFO, true).node();
        third.clusterId("cluster-b");
        third.markReady();
        ConnectorHub.Node foreign = hub.register("account-b", "node-bbbbbbbb", INFO, true).node();
        foreign.clusterId("cluster-foreign");
        foreign.markReady();
        assertThat(hub.onlineNodes("account-a")).hasSize(3);
        assertThat(hub.onlineNodes("account-b")).containsExactly(foreign);

        List<String>[] firstPoll = new List[1];
        List<String>[] secondPoll = new List[1];
        Disposable pollA = hub.awaitRequests(first).subscribe(lines -> firstPoll[0] = lines);
        Disposable pollB = hub.awaitRequests(second).subscribe(lines -> secondPoll[0] = lines);
        try {
            StepVerifier.create(hub.send("account-a", "cluster-b", "GET", "/api/version"))
                    .then(() -> {
                        assertThat(firstPoll[0] == null || firstPoll[0].isEmpty()).isTrue();
                        assertThat(secondPoll[0]).hasSize(1);
                        String requestId = secondPoll[0].getFirst().split(" ")[0];
                        assertThat(hub.complete("account-b", "node-bbbbbbbb", requestId, 200, "{}")).isFalse();
                        assertThat(hub.complete("account-a", "node-aaaaaaaa", requestId, 200, "{}")).isFalse();
                        assertThat(hub.complete("account-a", "node-bbbbbbbb", requestId, 200, "{}")).isTrue();
                    })
                    .assertNext(response -> assertThat(response.status()).isEqualTo(200))
                    .verifyComplete();

            StepVerifier.create(hub.send("account-a", "cluster-foreign", "GET", "/api/version"))
                    .expectError(ConnectorHub.ClusterNotConnectedException.class).verify();
            StepVerifier.create(hub.send("account-a", "cluster-unknown", "GET", "/api/version"))
                    .expectError(ConnectorHub.ClusterNotConnectedException.class).verify();
        } finally {
            pollA.dispose();
            pollB.dispose();
        }
    }

    @Test
    void aServiceWideNodeCeilingReclaimsStaleNodesAndProtectsOtherAccounts() {
        ConnectorHub limited = new ConnectorHub(new HubProperties("http://localhost", "hub@test", null,
                Duration.ofSeconds(2), Duration.ofMillis(500), Duration.ofMinutes(30), Duration.ofDays(1)), 3);
        ConnectorHub.Node first = limited.register("pro-a", "node-aaaaaaaa", INFO, true).node();
        limited.register("pro-a", "node-bbbbbbbb", INFO, true);
        limited.register("pro-b", "node-cccccccc", INFO, true);
        assertThatThrownBy(() -> limited.register("pro-b", "node-dddddddd", INFO, true))
                .isInstanceOf(ConnectorHub.GlobalNodeLimitException.class);

        first.markPolledAt(Instant.now().minusSeconds(31));
        assertThat(limited.register("pro-b", "node-dddddddd", INFO, true).isNew()).isTrue();
        assertThat(limited.onlineNodes("pro-a")).hasSize(1);
        assertThat(limited.onlineNodes("pro-b")).hasSize(2);
    }

    @Test
    void anOfflineSelectedClusterNeverFallsBackToAnotherProCluster() {
        ConnectorHub.Node offline = hub.register("account-a", "node-aaaaaaaa", INFO, true).node();
        offline.clusterId("cluster-offline");
        offline.markReady();
        offline.markPolledAt(Instant.now().minusSeconds(30));
        ConnectorHub.Node online = hub.register("account-a", "node-bbbbbbbb", INFO, true).node();
        online.clusterId("cluster-online");
        online.markReady();

        StepVerifier.create(hub.send("account-a", "cluster-offline", "GET", "/api/jobs"))
                .expectError(ConnectorHub.ClusterNotConnectedException.class).verify();
        StepVerifier.create(hub.awaitRequests(online))
                .assertNext(lines -> assertThat(lines).isEmpty()).verifyComplete();
    }

    @Test
    void readOnlyNodeCannotReceiveAMutationEvenWhenCalledDirectly() {
        ConnectorHub.Node node = hub.register("account-a", "node-aaaaaaaa",
                new ConnectorHub.NodeInfo("9.0.0", "21", "spring-boot", false, false), true).node();
        node.clusterId("cluster-a");
        node.markReady();

        StepVerifier.create(hub.send("account-a", "cluster-a", "POST", "/api/jobs/0199aaaa-0000-7000-8000-000000000001/requeue"))
                .expectError(ConnectorHub.ReadOnlyConnectorException.class).verify();
        StepVerifier.create(hub.sendTo(node, "POST", "/api/jobs/0199aaaa-0000-7000-8000-000000000001/requeue"))
                .expectError(ConnectorHub.ReadOnlyConnectorException.class).verify();
    }

    @Test
    void oneNodeCannotOpenConcurrentPolls() {
        ConnectorHub.Node node = hub.register("account-a", "node-aaaaaaaa", INFO, true).node();
        Disposable poll = hub.awaitRequests(node).subscribe();
        try {
            StepVerifier.create(hub.awaitRequests(node))
                    .expectError(ConnectorHub.AlreadyPollingException.class).verify();
        } finally {
            poll.dispose();
        }
    }

    @Test
    void pollBatchAndPendingRequestsAreBounded() {
        ConnectorHub.Node node = hub.register("account-a", "node-aaaaaaaa", INFO, true).node();
        List<Disposable> requests = new ArrayList<>();
        try {
            for (int i = 0; i < 256; i++) {
                requests.add(hub.sendTo(node, "GET", "/api/version", Duration.ofMinutes(1)).subscribe());
            }
            StepVerifier.create(hub.sendTo(node, "GET", "/api/version", Duration.ofMinutes(1)))
                    .expectError(ConnectorHub.TooManyPendingRequestsException.class).verify();
            StepVerifier.create(hub.awaitRequests(node))
                    .assertNext(lines -> assertThat(lines).hasSize(64)).verifyComplete();
            StepVerifier.create(hub.awaitRequests(node))
                    .assertNext(lines -> assertThat(lines).hasSize(64)).verifyComplete();
        } finally {
            requests.forEach(Disposable::dispose);
        }
    }

    @Test
    void aSlowDeliveredRequestKeepsItsNodeLiveUntilTheResultTimeout() {
        ConnectorHub.Node node = hub.register("account-a", "node-aaaaaaaa", INFO, true).node();
        Disposable request = hub.sendTo(node, "GET", "/api/version", Duration.ofMinutes(1)).subscribe();
        try {
            node.markPolledAt(Instant.now().minus(Duration.ofMinutes(6)));
            assertThat(hub.onlineNodes("account-a")).containsExactly(node);
            hub.forgetDisconnectedNodes();
            assertThat(hub.register("account-a", "node-aaaaaaaa", INFO, true).node()).isSameAs(node);
        } finally {
            request.dispose();
        }
        node.markPolledAt(Instant.now().minusSeconds(60));
        assertThat(hub.onlineNodes("account-a")).isEmpty();
    }
}
