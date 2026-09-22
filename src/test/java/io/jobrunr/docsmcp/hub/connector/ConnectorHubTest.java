package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorHubTest {

    private static final ConnectorHub.NodeInfo INFO = new ConnectorHub.NodeInfo("9.0.0", "21", "spring-boot", true);

    private final ConnectorHub hub = new ConnectorHub(new HubProperties("http://localhost", "hub@test", null,
            Duration.ofSeconds(2), Duration.ofMillis(500), Duration.ofMinutes(30), Duration.ofDays(1)));

    @Test
    void aPollingNodeGetsTheRequestAndItsAnswerCompletesTheCall() {
        ConnectorHub.Node node = hub.register("account-1", "node-aaaaaaaa", INFO).node();
        List<String>[] handedOut = new List[1];
        Disposable poll = hub.awaitRequests(node).subscribe(lines -> handedOut[0] = lines);

        StepVerifier.create(hub.send("account-1", null, "GET", "/api/servers"))
                .then(() -> {
                    assertThat(handedOut[0]).hasSize(1);
                    String requestId = handedOut[0].getFirst().split(" ")[0];
                    assertThat(handedOut[0].getFirst()).endsWith(" GET /api/servers");
                    assertThat(hub.complete("other-account", requestId, 200, "[]")).isFalse();
                    assertThat(hub.complete("account-1", requestId, 200, "[]")).isTrue();
                })
                .assertNext(response -> assertThat(response.body()).isEqualTo("[]"))
                .verifyComplete();
        poll.dispose();
    }

    @Test
    void aNodeThatStoppedPollingIsNotOnlineAndDoesNotGetRequests() {
        ConnectorHub.Node dead = hub.register("account-1", "node-dead0000", INFO).node();
        dead.clusterId("old-cluster");
        dead.markPolledAt(Instant.now().minusSeconds(30));
        ConnectorHub.Node alive = hub.register("account-1", "node-alive000", INFO).node();
        alive.clusterId("new-cluster");
        List<String>[] handedOut = new List[1];
        Disposable poll = hub.awaitRequests(alive).subscribe(lines -> handedOut[0] = lines);

        assertThat(hub.onlineNodes("account-1")).containsExactly(alive);
        // even though the account is still linked to the old cluster, the live node answers
        hub.send("account-1", "old-cluster", "GET", "/api/version").subscribe();
        assertThat(handedOut[0]).hasSize(1);
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
}
