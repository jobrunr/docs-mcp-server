package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InstanceMonitorTransientMetadataTest {

    @Test
    void missingStartupMetadataIsRetriedBeforeTheNodeBecomesRoutable() {
        ConnectorHub realHub = new ConnectorHub(new HubProperties("http://localhost", "hub@test", null,
                Duration.ofSeconds(2), Duration.ofSeconds(10), Duration.ofMinutes(30), Duration.ofDays(1)), 10_000);
        ConnectorHub.Node node = realHub.register("account-1", "node-1",
                new ConnectorHub.NodeInfo("9.0.0", "21", "test", false), false).node();
        ConnectorHub hub = mock(ConnectorHub.class);
        AccountStore store = mock(AccountStore.class);
        Account account = new Account("account-1", "owner@example.test", "Owner", "Example", null, null,
                "cluster-one", Instant.now(), Instant.now(), false);
        when(store.findById(account.id())).thenReturn(Optional.of(account));
        when(store.hasLeadEvent(eq(account.id()), anyString())).thenReturn(true);
        AtomicInteger metadataCalls = new AtomicInteger();
        when(hub.sendTo(eq(node), eq("GET"), anyString())).thenAnswer(invocation -> {
            String path = invocation.getArgument(2);
            if ("/api/metadata/id/cluster".equals(path)) {
                if (metadataCalls.getAndIncrement() == 0) return Mono.just(new ConnectorHub.TunnelResponse(404, ""));
                return Mono.just(new ConnectorHub.TunnelResponse(200,
                        "{\"name\":\"id\",\"owner\":\"cluster\",\"value\":\"cluster-one\"}"));
            }
            return Mono.just(new ConnectorHub.TunnelResponse(404, ""));
        });

        InstanceMonitor monitor = new InstanceMonitor(hub, store, mock(LeadEvents.class), new ObjectMapper());
        monitor.refresh(node);
        assertThat(node.rejectedReason()).isNull();
        assertThat(node.isReady()).isFalse();

        monitor.refresh(node);
        assertThat(node.isReady()).isTrue();
        assertThat(node.clusterId()).isEqualTo("cluster-one");
        assertThat(metadataCalls).hasValue(2);
        verify(store, times(1)).saveInstance(any());
    }
}
