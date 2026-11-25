package io.jobrunr.docsmcp.hub.mcp;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.connector.ConnectorHub;
import io.jobrunr.docsmcp.hub.connector.DashboardClient;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OpsToolsPreviewTest {

    private static final String JOB_ID = "0199aaaa-0000-7000-8000-000000000001";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void aPreviewHasExactlyOneWinnerUnderConcurrentExecution() throws Exception {
        DashboardClient dashboard = mock(DashboardClient.class);
        when(dashboard.getAll(any(Account.class), anyList())).thenReturn(List.of(job("FAILED")));
        when(dashboard.executeAll(any(Account.class), eq("POST"), anyList()))
                .thenReturn(Collections.singletonList(null));
        OpsTools tools = tools(dashboard);
        BarrierPreviewMap previews = new BarrierPreviewMap();
        previews.put("preview", preview("account-a", "cluster-a"));
        setPreviewMap(tools, previews);
        Account account = account("account-a", "cluster-a");

        CompletableFuture<Object> first = CompletableFuture.supplyAsync(() -> tryExecute(tools, account, "preview"));
        CompletableFuture<Object> second = CompletableFuture.supplyAsync(() -> tryExecute(tools, account, "preview"));
        List<Object> outcomes = List.of(first.get(3, TimeUnit.SECONDS), second.get(3, TimeUnit.SECONDS));
        assertThat(outcomes.stream().filter(Map.class::isInstance)).hasSize(1);
        assertThat(outcomes.stream().filter(RuntimeException.class::isInstance)).hasSize(1);
        verify(dashboard, times(1)).executeAll(eq(account), eq("POST"),
                eq(List.of("/api/jobs/" + JOB_ID + "/requeue")));
    }

    @Test
    void previewCannotBeMovedToAnotherClusterAndChangedStateIsNotRequeued() throws Exception {
        DashboardClient dashboard = mock(DashboardClient.class);
        when(dashboard.getAll(any(Account.class), anyList())).thenReturn(List.of(job("ENQUEUED")));
        OpsTools tools = tools(dashboard);
        previewMap(tools).put("preview", preview("account-a", "cluster-a"));

        assertThatThrownBy(() -> execute(tools, account("account-a", "cluster-b"), "preview"))
                .hasMessageContaining("cluster_id differs");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) execute(tools, account("account-a", "cluster-a"), "preview");
        assertThat(result).containsEntry("requeued", 0);
        assertThat((List<?>) result.get("failed")).hasSize(1);
        verify(dashboard, never()).executeAll(any(Account.class), eq("POST"), anyList());
    }

    @Test
    void activePreviewCountIsBoundedPerAccount() throws Exception {
        DashboardClient dashboard = mock(DashboardClient.class);
        when(dashboard.getAll(any(Account.class), anyList())).thenReturn(List.of(job("FAILED")));
        OpsTools tools = tools(dashboard);
        Account account = account("account-a", "cluster-a");
        for (int i = 0; i < 20; i++) invoke(tools, "previewRequeue", account, Set.of(JOB_ID));
        assertThat(previewMap(tools)).hasSize(20);
        assertThatThrownBy(() -> invoke(tools, "previewRequeue", account, Set.of(JOB_ID)))
                .hasMessageContaining("Too many active requeue previews");
    }

    private static JsonNode job(String state) {
        return MAPPER.readTree("{\"jobHistory\":[{\"state\":\"" + state + "\"}]}");
    }

    private static Account account(String id, String clusterId) {
        return new Account(id, id + "@example.test", "Test", "Example", null, null,
                clusterId, Instant.now(), Instant.now(), true);
    }

    private static OpsTools tools(DashboardClient dashboard) {
        return new OpsTools(dashboard, mock(ConnectorHub.class), mock(AccountStore.class),
                mock(LeadEvents.class), mock(QueryLogClient.class), MAPPER, mock(JsonMapper.class), "test");
    }

    private static Object preview(String accountId, String clusterId) throws Exception {
        Class<?> type = Class.forName("io.jobrunr.docsmcp.hub.mcp.OpsTools$RequeuePreview");
        Constructor<?> constructor = type.getDeclaredConstructor(String.class, String.class, Set.class, List.class, Instant.class);
        constructor.setAccessible(true);
        return constructor.newInstance(accountId, clusterId, Set.of(JOB_ID), List.of(JOB_ID), Instant.now());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> previewMap(OpsTools tools) throws Exception {
        Field field = OpsTools.class.getDeclaredField("previews");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(tools);
    }

    private static void setPreviewMap(OpsTools tools, Map<String, Object> map) throws Exception {
        Field field = OpsTools.class.getDeclaredField("previews");
        field.setAccessible(true);
        field.set(tools, map);
    }

    private static Object tryExecute(OpsTools tools, Account account, String previewId) {
        try {
            return execute(tools, account, previewId);
        } catch (RuntimeException error) {
            return error;
        }
    }

    private static Object execute(OpsTools tools, Account account, String previewId) {
        return invoke(tools, "executeRequeue", account, Set.of(JOB_ID), previewId);
    }

    private static Object invoke(OpsTools tools, String name, Object... arguments) {
        try {
            Class<?>[] signature = name.equals("executeRequeue")
                    ? new Class<?>[]{Account.class, Set.class, String.class}
                    : new Class<?>[]{Account.class, Set.class};
            Method method = OpsTools.class.getDeclaredMethod(name, signature);
            method.setAccessible(true);
            return method.invoke(tools, arguments);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException error) throw error;
            throw new AssertionError(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static final class BarrierPreviewMap extends ConcurrentHashMap<String, Object> {
        private final CyclicBarrier bothRead = new CyclicBarrier(2);

        @Override
        public Object get(Object key) {
            Object value = super.get(key);
            if (value != null) {
                try {
                    bothRead.await(3, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
            return value;
        }
    }
}
