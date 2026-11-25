package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class InstanceMonitorQueueTest {

    @Test
    void ninthHealthyNodeIsRefreshedAsSoonAsASnapshotSlotIsFree() throws Exception {
        ConnectorHub hub = new ConnectorHub(new HubProperties("http://localhost", "hub@test", null,
                Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofMinutes(30), Duration.ofDays(1)), 10_000);
        CountDownLatch firstEightStarted = new CountDownLatch(8);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch allNineFinished = new CountDownLatch(9);
        InstanceMonitor monitor = new InstanceMonitor(hub, mock(AccountStore.class), mock(LeadEvents.class), new ObjectMapper()) {
            @Override
            void refresh(ConnectorHub.Node node) {
                firstEightStarted.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    allNineFinished.countDown();
                }
            }
        };

        for (int i = 0; i < 9; i++) {
            monitor.onNodeConnected(hub.register("pro-account", "node-queue-" + i,
                    new ConnectorHub.NodeInfo("9.0.0", "21", "test", false), true).node());
        }
        assertThat(firstEightStarted.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(allNineFinished.getCount()).isEqualTo(9);
        release.countDown();
        assertThat(allNineFinished.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
