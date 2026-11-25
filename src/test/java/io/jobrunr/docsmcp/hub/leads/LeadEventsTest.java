package io.jobrunr.docsmcp.hub.leads;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class LeadEventsTest {

    @Test
    void aRepeatedSecondClusterAttemptCreatesOnlyOneLeadEvent() {
        AccountStore store = mock(AccountStore.class);
        LeadEvents leads = leads(store);
        Account account = account();

        leads.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b");
        leads.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-c");

        verify(store, times(1)).recordLeadEvent(account.id(), LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b");
        verifyNoMoreInteractions(store);
    }

    @Test
    void aRepeatedProFeatureAttemptDoesNotFloodLeadsButDifferentFeaturesAreReported() {
        AccountStore store = mock(AccountStore.class);
        LeadEvents leads = leads(store);
        Account account = account();

        leads.record(account, LeadEvents.PRO_FEATURE_ATTEMPT, "search_jobs");
        leads.record(account, LeadEvents.PRO_FEATURE_ATTEMPT, "search_jobs");
        leads.record(account, LeadEvents.PRO_FEATURE_ATTEMPT, "get_job_stats");

        verify(store, times(1)).recordLeadEvent(account.id(), LeadEvents.PRO_FEATURE_ATTEMPT, "search_jobs");
        verify(store, times(1)).recordLeadEvent(account.id(), LeadEvents.PRO_FEATURE_ATTEMPT, "get_job_stats");
    }

    @Test
    void aFailedStoreWriteDoesNotSuppressTheNextAttempt() {
        AccountStore store = mock(AccountStore.class);
        Account account = account();
        doThrow(new IllegalStateException("temporary database failure")).doNothing().when(store)
                .recordLeadEvent(account.id(), LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b");
        LeadEvents leads = leads(store);

        assertThrows(IllegalStateException.class,
                () -> leads.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b"));
        leads.record(account, LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b");

        verify(store, times(2)).recordLeadEvent(account.id(), LeadEvents.SECOND_CLUSTER_ATTEMPT, "cluster-b");
    }

    private static LeadEvents leads(AccountStore store) {
        HubProperties properties = new HubProperties("http://localhost", "JobRunr <noreply@example.invalid>",
                "", Duration.ofSeconds(25), Duration.ofSeconds(20), Duration.ofMinutes(30), Duration.ofDays(30));
        return new LeadEvents(store, properties, mock(QueryLogClient.class), "local-test");
    }

    private static Account account() {
        return new Account("account-1", "owner@example.invalid", "Owner", "Example", null, null,
                "cluster-a", Instant.now(), Instant.now(), false);
    }
}
