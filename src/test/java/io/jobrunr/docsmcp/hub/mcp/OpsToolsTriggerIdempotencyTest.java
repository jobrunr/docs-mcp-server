package io.jobrunr.docsmcp.hub.mcp;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.connector.ConnectorHub;
import io.jobrunr.docsmcp.hub.connector.DashboardClient;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class OpsToolsTriggerIdempotencyTest {

    private static final String REQUEST_ID = "0199aaaa-0000-7000-8000-000000000001";
    private static final String TRIGGER_PATH = "/api/recurring-jobs/daily-report/trigger";

    @Test
    void retryReturnsTheFirstResultWithoutTriggeringAgain() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");

        Object first = tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID));
        Object retry = tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID));

        assertThat(first).isEqualTo(Map.of("triggered", "daily-report"));
        assertThat(retry).isEqualTo(first);
        verify(dashboard, times(1)).execute(account, "POST", TRIGGER_PATH);
    }

    @Test
    void sameRequestIdCannotBeMovedToAnotherRecurringJobOrCluster() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");
        tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID));

        assertThatThrownBy(() -> tools.triggerRecurringJob(account, args("other-report", REQUEST_ID)))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("different cluster or recurring job");
        assertThatThrownBy(() -> tools.triggerRecurringJob(account("account-a", "cluster-b"), args("daily-report", REQUEST_ID)))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("different cluster or recurring job");
        verify(dashboard, times(1)).execute(any(Account.class), eq("POST"), eq(TRIGGER_PATH));
    }

    @Test
    void sameRequestIdInAnotherAccountHasAnIndependentReceipt() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account accountA = account("account-a", "cluster-a");
        Account accountB = account("account-b", "cluster-b");

        tools.triggerRecurringJob(accountA, args("daily-report", REQUEST_ID));
        tools.triggerRecurringJob(accountB, args("daily-report", REQUEST_ID));

        verify(dashboard).execute(accountA, "POST", TRIGGER_PATH);
        verify(dashboard).execute(accountB, "POST", TRIGGER_PATH);
    }

    @Test
    void concurrentRetriesWaitForTheFirstCallAndExecuteOnlyOnce() throws Exception {
        DashboardClient dashboard = mock(DashboardClient.class);
        CountDownLatch executing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            executing.countDown();
            assertThat(release.await(3, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(dashboard).execute(any(Account.class), eq("POST"), eq(TRIGGER_PATH));
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Object> first = pool.submit(() -> tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID)));
            assertThat(executing.await(3, TimeUnit.SECONDS)).isTrue();
            CountDownLatch retryStarted = new CountDownLatch(1);
            Future<Object> retry = pool.submit(() -> {
                retryStarted.countDown();
                return tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID));
            });
            assertThat(retryStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(retry.isDone()).isFalse();
            release.countDown();

            assertThat(first.get(3, TimeUnit.SECONDS)).isEqualTo(Map.of("triggered", "daily-report"));
            assertThat(retry.get(3, TimeUnit.SECONDS)).isEqualTo(Map.of("triggered", "daily-report"));
            verify(dashboard, times(1)).execute(account, "POST", TRIGGER_PATH);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void uncertainFailureIsCachedAndDoesNotTriggerAgain() {
        DashboardClient dashboard = mock(DashboardClient.class);
        doThrow(new DashboardClient.DashboardException(504, "connector result was lost"))
                .when(dashboard).execute(any(Account.class), eq("POST"), eq(TRIGGER_PATH));
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");

        assertThatThrownBy(() -> tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID)))
                .isInstanceOf(DashboardClient.DashboardException.class).hasMessageContaining("result was lost");
        assertThatThrownBy(() -> tools.triggerRecurringJob(account, args("daily-report", REQUEST_ID)))
                .isInstanceOf(DashboardClient.DashboardException.class).hasMessageContaining("result was lost");
        verify(dashboard, times(1)).execute(account, "POST", TRIGGER_PATH);
    }

    @Test
    void completedReceiptsExpireAndPerAccountMemoryIsBounded() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");
        for (int i = 0; i < OpsTools.MAX_TRIGGER_RECEIPTS_PER_ACCOUNT; i++) {
            tools.triggerRecurringJob(account, args("daily-report", "request-" + i));
        }

        assertThatThrownBy(() -> tools.triggerRecurringJob(account, args("daily-report", "request-over-cap")))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("Too many recent recurring-job triggers");
        tools.expireTriggerReceipts(Instant.now().plus(OpsTools.TRIGGER_RECEIPT_TTL).plusSeconds(1));
        tools.triggerRecurringJob(account, args("daily-report", "request-over-cap"));

        verify(dashboard, times(OpsTools.MAX_TRIGGER_RECEIPTS_PER_ACCOUNT + 1))
                .execute(account, "POST", TRIGGER_PATH);
    }

    @Test
    void globalReceiptMemoryIsBounded() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        for (int i = 0; i < OpsTools.MAX_TRIGGER_RECEIPTS_GLOBAL; i++) {
            tools.triggerRecurringJob(account("account-" + i, "cluster-a"), args("daily-report", REQUEST_ID));
        }

        assertThatThrownBy(() -> tools.triggerRecurringJob(account("account-over-cap", "cluster-a"), args("daily-report", REQUEST_ID)))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("Too many recent recurring-job triggers");
    }

    @Test
    void omittingRequestIdPreservesEachCallAsANewTrigger() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");

        tools.triggerRecurringJob(account, Map.of("recurring_job_id", "daily-report"));
        tools.triggerRecurringJob(account, Map.of("recurring_job_id", "daily-report"));

        verify(dashboard, times(2)).execute(account, "POST", TRIGGER_PATH);
    }

    @Test
    void malformedOrExplicitlyNullRequestIdCannotSilentlyTriggerWithoutAReceipt() {
        DashboardClient dashboard = mock(DashboardClient.class);
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");
        Map<String, Object> nullId = new HashMap<>();
        nullId.put("recurring_job_id", "daily-report");
        nullId.put("request_id", null);

        assertThatThrownBy(() -> tools.triggerRecurringJob(account, args("daily-report", "short")))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("request_id must be");
        assertThatThrownBy(() -> tools.triggerRecurringJob(account, nullId))
                .isInstanceOf(OpsTools.ToolException.class).hasMessageContaining("request_id must be");
        verify(dashboard, times(0)).execute(any(Account.class), eq("POST"), eq(TRIGGER_PATH));
    }

    private static Map<String, Object> args(String recurringJobId, String requestId) {
        return Map.of("recurring_job_id", recurringJobId, "request_id", requestId);
    }

    private static Account account(String id, String clusterId) {
        return new Account(id, id + "@example.test", "Test", "Example", null, null,
                clusterId, Instant.EPOCH, Instant.EPOCH, true);
    }

    private static OpsTools tools(DashboardClient dashboard) {
        return new OpsTools(dashboard, mock(ConnectorHub.class), mock(AccountStore.class), mock(LeadEvents.class),
                mock(QueryLogClient.class), new ObjectMapper(), mock(JsonMapper.class), "test");
    }
}
