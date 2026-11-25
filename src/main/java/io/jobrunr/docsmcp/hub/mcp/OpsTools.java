package io.jobrunr.docsmcp.hub.mcp;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.connector.ConnectorHub;
import io.jobrunr.docsmcp.hub.connector.DashboardClient;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import io.jobrunr.docsmcp.logging.QueryLogEntry;
import io.jobrunr.docsmcp.logging.RequestContextHolder;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ToolAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * The tools that operate a JobRunr cluster. They run in the hub: each tool turns into one or more dashboard API calls
 * that the connector in the customer's JVM executes. Aggregation (overview, triage, previews) happens here, so the
 * connector stays a relay.
 */
@Component
public class OpsTools {

    public static final String ACCOUNT_KEY = "jobrunr.account";
    public static final String USER_AGENT_KEY = "jobrunr.userAgent";

    static final int MAX_BULK_REQUEUE = 100;
    private static final int MAX_LIST_OFFSET = 10_000;
    private static final int MAX_PREVIEWS_PER_ACCOUNT = 20;
    private static final int MAX_PREVIEWS_GLOBAL = 2_000;
    static final int MAX_TRIGGER_RECEIPTS_PER_ACCOUNT = 100;
    static final int MAX_TRIGGER_RECEIPTS_GLOBAL = 2_000;
    private static final int MAX_STRING_LENGTH = 6_000;
    private static final int MAX_HISTORY = 10;
    private static final Duration PREVIEW_TTL = Duration.ofMinutes(15);
    static final Duration TRIGGER_RECEIPT_TTL = Duration.ofMinutes(15);
    private static final Duration TRIGGER_REPLAY_WAIT = Duration.ofSeconds(30);
    private static final List<String> STATES = List.of("AWAITING", "SCHEDULED", "ENQUEUED", "PROCESSING", "FAILED", "SUCCEEDED", "DELETED");
    private static final List<String> OVERVIEW_STATES = List.of("SCHEDULED", "ENQUEUED", "PROCESSING", "FAILED", "SUCCEEDED");
    private static final Set<String> REQUEUEABLE_STATES = Set.of("FAILED", "SUCCEEDED", "DELETED", "SCHEDULED");
    private static final Pattern JOB_ID = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern RECURRING_JOB_ID = Pattern.compile("[\\dA-Za-z\\-_(),.]{1,127}");
    private static final Pattern TRIGGER_REQUEST_ID = Pattern.compile("[A-Za-z0-9_-]{8,128}");
    private static final Pattern PROBLEM_TYPE = Pattern.compile("[a-z0-9-]{1,64}");
    private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final Pattern VOLATILE_PARTS = Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|\\d+");
    private static final String PRO_URL = "https://www.jobrunr.io/en/pro/";

    private static final Logger log = LoggerFactory.getLogger(OpsTools.class);

    private final DashboardClient dashboard;
    private final ConnectorHub hub;
    private final AccountStore store;
    private final LeadEvents leadEvents;
    private final QueryLogClient queryLogClient;
    private final String serverVersion;
    private final ObjectMapper objectMapper;
    private final McpJsonMapper mcpJsonMapper;
    private final Map<String, RequeuePreview> previews = new ConcurrentHashMap<>();
    /** Guarded by itself so a receipt is checked and reserved atomically. */
    private final Map<TriggerRequestKey, TriggerReceipt> triggerReceipts = new HashMap<>();

    public OpsTools(DashboardClient dashboard, ConnectorHub hub, AccountStore store, LeadEvents leadEvents, QueryLogClient queryLogClient,
                    ObjectMapper objectMapper, @Qualifier("mcpServerJsonMapper") JsonMapper mcpServerJsonMapper,
                    @Value("${spring.ai.mcp.server.version:unknown}") String serverVersion) {
        this.dashboard = dashboard;
        this.hub = hub;
        this.store = store;
        this.leadEvents = leadEvents;
        this.queryLogClient = queryLogClient;
        this.serverVersion = serverVersion;
        this.objectMapper = objectMapper;
        this.mcpJsonMapper = new JacksonMcpJsonMapper(mcpServerJsonMapper);
    }

    @FunctionalInterface
    interface ToolHandler {
        Object handle(Account account, Map<String, Object> arguments);
    }

    /** A message for the agent, returned as a tool error. */
    static class ToolException extends RuntimeException {
        ToolException(String message) {
            super(message);
        }
    }

    private record RequeuePreview(String accountId, String clusterId, Set<String> requestedIds,
                                  List<String> requeueableIds, Instant createdAt) {
    }

    private record TriggerRequestKey(String accountId, String requestId) {
    }

    private record TriggerOutcome(Object value, RuntimeException failure) {
    }

    private static final class TriggerReceipt {
        private final String clusterId;
        private final String recurringJobId;
        private final CompletableFuture<TriggerOutcome> outcome = new CompletableFuture<>();
        private volatile Instant completedAt;

        private TriggerReceipt(String clusterId, String recurringJobId) {
            this.clusterId = clusterId;
            this.recurringJobId = recurringJobId;
        }

        private void complete(TriggerOutcome result) {
            outcome.complete(result);
            completedAt = Instant.now();
        }
    }

    public List<SyncToolSpecification> specifications() {
        return List.of(
                tool("get_cluster_overview", "Cluster overview", """
                                Health of the user's JobRunr cluster in one call: JobRunr version, job counts per state, background \
                                job servers, recurring job count and open problems. Start here for any question about their jobs.""",
                        noArguments(), readOnly(), (account, args) -> clusterOverview(account)),
                tool("list_jobs", "List jobs", """
                                List jobs in one state, most recently updated first. Returns a compact summary per job (id, name, \
                                signature, state, timestamps and, for failed jobs, the exception). Use get_job for the full details.""",
                        """
                                {"type":"object","properties":{
                                  "state":{"type":"string","enum":["AWAITING","SCHEDULED","ENQUEUED","PROCESSING","FAILED","SUCCEEDED","DELETED"],"description":"Job state to list"},
                                  "offset":{"type":"integer","minimum":0,"maximum":10000,"description":"Number of jobs to skip (default 0, max 10000)"},
                                  "limit":{"type":"integer","minimum":1,"maximum":100,"description":"Number of jobs to return (default 20, max 100)"}},
                                 "required":["state"]}""",
                        readOnly(), this::listJobs),
                tool("get_job", "Get job", """
                                Full details of one job: signature, parameter types, labels, state history with the complete \
                                exception and stack trace of each failure, and dashboard log lines.""",
                        """
                                {"type":"object","properties":{"job_id":{"type":"string","description":"The job id (a UUID)"}},"required":["job_id"]}""",
                        readOnly(), (account, args) -> getJob(account, jobId(args, "job_id"))),
                tool("triage_failed_jobs", "Triage failed jobs", """
                                Groups the failed jobs by job signature and exception, biggest group first, with the job ids of \
                                each group. Use it during an incident to see what is failing and why, then requeue a group with \
                                requeue_jobs once the cause is fixed.""",
                        """
                                {"type":"object","properties":{"max_jobs":{"type":"integer","minimum":1,"maximum":2000,"description":"How many of the most recent failed jobs to analyze (default 500)"}}}""",
                        readOnly(), this::triageFailedJobs),
                tool("list_recurring_jobs", "List recurring jobs", """
                                Recurring jobs with their id, name, cron or interval schedule, time zone and next run.""",
                        """
                                {"type":"object","properties":{
                                  "offset":{"type":"integer","minimum":0,"maximum":10000,"description":"Number of recurring jobs to skip (default 0, max 10000)"},
                                  "limit":{"type":"integer","minimum":1,"maximum":200,"description":"Number to return (default 50)"}}}""",
                        readOnly(), this::listRecurringJobs),
                tool("list_servers", "List background job servers", """
                                The background job servers of the cluster with worker count, heartbeat, CPU load and memory.""",
                        noArguments(), readOnly(), (account, args) -> listServers(account)),
                tool("get_problems", "Get problems", """
                                Problems the JobRunr dashboard currently reports, like jobs whose class no longer exists or severe \
                                JobRunr exceptions.""",
                        noArguments(), readOnly(), (account, args) -> dashboard.get(account, "/api/problems")),
                tool("list_clusters", "List clusters", """
                                The JobRunr clusters connected to this account.""",
                        noArguments(), readOnly(), (account, args) -> listClusters(account)),
                tool("search_jobs", "Search jobs", """
                                Search jobs across all states by job name, label, exception text and time window, across clusters. \
                                This is a JobRunr Pro feature.""",
                        """
                                {"type":"object","properties":{
                                  "query":{"type":"string","description":"Text to search for in job names, labels and exceptions"},
                                  "state":{"type":"string","description":"Optional job state"}},"required":["query"]}""",
                        readOnly(), (account, args) -> proFeature(account, "search_jobs", "Job search",
                                "Searching jobs by name, label and exception across clusters is part of JobRunr Pro. Without Pro, "
                                        + "use list_jobs with a state or triage_failed_jobs to group failures.")),
                tool("requeue_job", "Requeue job", """
                                Enqueue one job again, for example a failed job whose cause has been fixed. Needs a connector \
                                token with operate access.""",
                        """
                                {"type":"object","properties":{"job_id":{"type":"string","description":"The job id (a UUID)"}},"required":["job_id"]}""",
                        annotations(false, true, false), (account, args) -> requeueJob(account, jobId(args, "job_id"))),
                tool("requeue_jobs", "Requeue jobs in bulk", """
                                Requeue up to %d jobs in two steps. First call with dry_run=true (the default): it checks every \
                                job and returns a preview_id. Show the preview to the user. Then call again with dry_run=false, \
                                the same job_ids and that preview_id. Needs a connector token with operate access.""".formatted(MAX_BULK_REQUEUE),
                        """
                                {"type":"object","properties":{
                                  "job_ids":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":%d,"description":"The ids of the jobs to requeue"},
                                  "dry_run":{"type":"boolean","description":"true (default) returns a preview, false executes a previous preview"},
                                  "preview_id":{"type":"string","description":"The preview_id returned by the dry run, required when dry_run is false"}},
                                 "required":["job_ids"]}""".formatted(MAX_BULK_REQUEUE),
                        annotations(false, true, false), this::requeueJobs),
                tool("delete_job", "Delete job", """
                                Delete one job (it moves to the DELETED state and is removed later). Needs a connector token with \
                                operate access. Only call this when the user explicitly asked to delete this job.""",
                        """
                                {"type":"object","properties":{"job_id":{"type":"string","description":"The job id (a UUID)"}},"required":["job_id"]}""",
                        annotations(false, true, true), (account, args) -> deleteJob(account, jobId(args, "job_id"))),
                tool("trigger_recurring_job", "Trigger recurring job", """
                                Run a recurring job now, outside its schedule. Needs a connector token with operate access. \
                                For safe retries, pass a fresh request_id for each intended run and reuse that same ID only \
                                when retrying an uncertain result. A request_id is remembered for 15 minutes.""",
                        """
                                {"type":"object","properties":{
                                  "recurring_job_id":{"type":"string","description":"The recurring job id from list_recurring_jobs"},
                                  "request_id":{"type":"string","minLength":8,"maxLength":128,"pattern":"^[A-Za-z0-9_-]+$","description":"Optional stable caller-generated ID (for example a UUID). Reuse only when retrying this trigger."}},
                                 "required":["recurring_job_id"]}""",
                        annotations(false, true, false), this::triggerRecurringJob),
                tool("dismiss_problem", "Dismiss problem", """
                                Dismiss a problem shown in the dashboard, by its type from get_problems. Needs a connector token \
                                with operate access.""",
                        """
                                {"type":"object","properties":{"problem_type":{"type":"string","description":"The problem type, for example severe-jobrunr-exception"}},"required":["problem_type"]}""",
                        annotations(false, true, true), this::dismissProblem));
    }

    // ------------------------------------------------------------------ read tools

    private Object clusterOverview(Account account) {
        List<ConnectorHub.Node> nodes = hub.onlineNodes(account.id()).stream()
                .filter(ConnectorHub.Node::isReady)
                .filter(node -> account.clusterId() == null || account.clusterId().equals(node.clusterId()))
                .toList();
        if (nodes.isEmpty()) throw new ConnectorHub.NoInstanceConnectedException();

        List<String> paths = new ArrayList<>(List.of("/api/version", "/api/servers", "/api/problems", "/api/recurring-jobs?offset=0&limit=1"));
        OVERVIEW_STATES.forEach(state -> paths.add("/api/jobs?state=" + state + "&offset=0&limit=1"));
        List<JsonNode> results = dashboard.getAll(account, paths);

        Map<String, Object> jobCounts = new LinkedHashMap<>();
        for (int i = 0; i < OVERVIEW_STATES.size(); i++) {
            jobCounts.put(OVERVIEW_STATES.get(i).toLowerCase(), total(results.get(4 + i)));
        }
        List<Object> servers = new ArrayList<>();
        if (results.get(1) != null) results.get(1).forEach(server -> servers.add(compactServer(server)));

        Map<String, Object> overview = new LinkedHashMap<>();
        overview.put("clusterId", account.clusterId());
        overview.put("jobrunrVersion", text(results.get(0), "version"));
        overview.put("connectedNodes", nodes.size());
        overview.put("jobParameterValues", nodes.getFirst().info().redaction() ? "redacted" : "included");
        overview.put("jobCounts", jobCounts);
        overview.put("backgroundJobServers", servers);
        overview.put("recurringJobs", total(results.get(3)));
        overview.put("problems", results.get(2));

        List<String> hints = new ArrayList<>();
        Object failed = jobCounts.get("failed");
        if (failed instanceof Long count && count > 0) hints.add(count + " failed jobs: call triage_failed_jobs to see what fails and why.");
        if (servers.isEmpty()) hints.add("No background job server is running, so no jobs are being processed.");
        if (results.get(2) != null && !results.get(2).isEmpty()) hints.add("The dashboard reports problems, see the problems field.");
        overview.put("hints", hints);
        return overview;
    }

    private Object listJobs(Account account, Map<String, Object> args) {
        String state = string(args, "state");
        if (state == null || !STATES.contains(state.toUpperCase())) throw new ToolException("state must be one of " + STATES);
        int offset = integer(args, "offset", 0, 0, MAX_LIST_OFFSET);
        int limit = integer(args, "limit", 20, 1, 100);
        JsonNode page = dashboard.get(account, "/api/jobs?state=" + state.toUpperCase() + "&offset=" + offset + "&limit=" + limit + "&order=updatedAt:DESC");

        List<Object> jobs = new ArrayList<>();
        page.path("items").forEach(job -> jobs.add(summary(job)));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("state", state.toUpperCase());
        result.put("total", total(page));
        result.put("offset", offset);
        result.put("jobs", jobs);
        result.put("hasMore", total(page) != null && offset + jobs.size() < total(page));
        return result;
    }

    private Object getJob(Account account, String jobId) {
        JsonNode job = dashboard.getOrNull(account, "/api/jobs/" + jobId);
        if (job == null) throw new ToolException("No job found with id " + jobId);
        ObjectNode trimmed = (ObjectNode) truncateStrings(job);
        JsonNode history = trimmed.path("jobHistory");
        if (history.isArray() && history.size() > MAX_HISTORY) {
            ArrayNode last = objectMapper.createArrayNode();
            for (int i = history.size() - MAX_HISTORY; i < history.size(); i++) last.add(history.get(i));
            trimmed.set("jobHistory", last);
            trimmed.put("olderStatesOmitted", history.size() - MAX_HISTORY);
        }
        trimmed.put("state", currentState(job));
        return trimmed;
    }

    private Object triageFailedJobs(Account account, Map<String, Object> args) {
        int maxJobs = integer(args, "max_jobs", 500, 1, 2000);
        JsonNode firstPage = dashboard.get(account, "/api/jobs?state=FAILED&offset=0&limit=100&order=updatedAt:DESC");
        Long totalFailed = total(firstPage);
        List<JsonNode> fetched = new ArrayList<>();
        firstPage.path("items").forEach(fetched::add);

        int toFetch = (int) Math.min(maxJobs, totalFailed == null ? 0 : totalFailed);
        List<String> nextPages = IntStream.iterate(100, offset -> offset < toFetch, offset -> offset + 100)
                .mapToObj(offset -> "/api/jobs?state=FAILED&offset=" + offset + "&limit=100&order=updatedAt:DESC")
                .toList();
        if (!nextPages.isEmpty()) {
            dashboard.getAll(account, nextPages).forEach(page -> {
                if (page != null) page.path("items").forEach(fetched::add);
            });
        }
        List<JsonNode> jobs = fetched.size() > maxJobs ? fetched.subList(0, maxJobs) : fetched;

        Map<String, Map<String, Object>> groups = new LinkedHashMap<>();
        for (JsonNode job : jobs) {
            JsonNode failure = lastFailure(job);
            String exceptionType = text(failure, "exceptionType");
            String exceptionMessage = text(failure, "exceptionMessage");
            String key = text(job, "jobSignature") + "|" + exceptionType + "|" + (exceptionMessage == null ? "" : VOLATILE_PARTS.matcher(exceptionMessage).replaceAll("#"));
            Map<String, Object> group = groups.computeIfAbsent(key, k -> {
                Map<String, Object> g = new LinkedHashMap<>();
                g.put("jobName", text(job, "jobName"));
                g.put("jobSignature", text(job, "jobSignature"));
                g.put("exceptionType", exceptionType);
                g.put("exceptionMessage", truncate(exceptionMessage, 500));
                g.put("exceptionCauseType", text(failure, "exceptionCauseType"));
                g.put("count", 0);
                g.put("lastFailedAt", text(failure, "createdAt"));
                g.put("firstFailedAt", text(failure, "createdAt"));
                g.put("jobIds", new ArrayList<String>());
                return g;
            });
            group.put("count", (Integer) group.get("count") + 1);
            group.put("firstFailedAt", text(failure, "createdAt"));
            @SuppressWarnings("unchecked") List<String> ids = (List<String>) group.get("jobIds");
            if (ids.size() < MAX_BULK_REQUEUE) ids.add(text(job, "id"));
        }

        List<Map<String, Object>> sorted = groups.values().stream()
                .sorted((a, b) -> Integer.compare((Integer) b.get("count"), (Integer) a.get("count")))
                .toList();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalFailedJobs", totalFailed);
        result.put("analyzedJobs", jobs.size());
        result.put("groups", sorted);
        result.put("hint", sorted.isEmpty()
                ? "There are no failed jobs."
                : "Each group lists up to " + MAX_BULK_REQUEUE + " job ids. Fix the cause first, then requeue a group with requeue_jobs (dry run first).");
        return result;
    }

    private Object listRecurringJobs(Account account, Map<String, Object> args) {
        int offset = integer(args, "offset", 0, 0, MAX_LIST_OFFSET);
        int limit = integer(args, "limit", 50, 1, 200);
        JsonNode page = dashboard.get(account, "/api/recurring-jobs?offset=" + offset + "&limit=" + limit);
        List<Object> recurringJobs = new ArrayList<>();
        page.path("items").forEach(job -> {
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("id", text(job, "id"));
            summary.put("jobName", text(job, "jobName"));
            summary.put("jobSignature", text(job, "jobSignature"));
            summary.put("schedule", text(job, "scheduleExpression"));
            summary.put("zoneId", text(job, "zoneId"));
            summary.put("nextRun", text(job, "nextRun"));
            summary.put("labels", job.path("labels"));
            recurringJobs.add(summary);
        });
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", total(page));
        result.put("offset", offset);
        result.put("recurringJobs", recurringJobs);
        return result;
    }

    private Object listServers(Account account) {
        List<Object> servers = new ArrayList<>();
        dashboard.get(account, "/api/servers").forEach(server -> servers.add(compactServer(server)));
        return Map.of("servers", servers);
    }

    private Object listClusters(Account account) {
        List<ConnectorHub.Node> nodes = hub.onlineNodes(account.id()).stream()
                .filter(ConnectorHub.Node::isReady)
                .filter(node -> account.proEnabled() || account.clusterId() == null || account.clusterId().equals(node.clusterId()))
                .toList();
        Map<String, List<ConnectorHub.Node>> byCluster = new LinkedHashMap<>();
        if (account.clusterId() != null) byCluster.put(account.clusterId(), new ArrayList<>());
        nodes.forEach(node -> byCluster.computeIfAbsent(node.clusterId(), ignored -> new ArrayList<>()).add(node));
        List<Map<String, Object>> clusters = new ArrayList<>();
        byCluster.forEach((id, members) -> {
            Map<String, Object> cluster = new LinkedHashMap<>();
            cluster.put("clusterId", id);
            cluster.put("connected", !members.isEmpty());
            cluster.put("connectedNodes", members.size());
            cluster.put("default", Objects.equals(id, account.clusterId()));
            cluster.put("jobrunrVersion", members.isEmpty() ? null : members.getFirst().info().jobrunrVersion());
            clusters.add(cluster);
        });
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("clusters", clusters);
        result.put("proEnabled", account.proEnabled());
        result.put("note", account.proEnabled()
                ? "JobRunr Pro allows unlimited connected nodes and clusters. Pass cluster_id on a cluster tool to select an online cluster."
                : "The free JobRunr MCP allows one cluster with up to two live nodes. JobRunr Pro allows unlimited connected nodes and clusters (" + PRO_URL + ").");
        return result;
    }

    // ------------------------------------------------------------------ operate tools

    private Object requeueJob(Account account, String jobId) {
        dashboard.execute(account, "POST", "/api/jobs/" + jobId + "/requeue");
        return Map.of("requeued", jobId);
    }

    private Object requeueJobs(Account account, Map<String, Object> args) {
        Object rawIds = args.get("job_ids");
        if (!(rawIds instanceof Collection<?> collection) || collection.isEmpty()) throw new ToolException("job_ids must be a non-empty array of job ids");
        if (collection.size() > MAX_BULK_REQUEUE) throw new ToolException("At most " + MAX_BULK_REQUEUE + " jobs per call.");
        Set<String> jobIds = new LinkedHashSet<>();
        for (Object id : collection) {
            if (!(id instanceof String s) || !JOB_ID.matcher(s).matches()) throw new ToolException("Not a job id: " + id);
            jobIds.add(s.toLowerCase());
        }
        if (jobIds.size() > MAX_BULK_REQUEUE) throw new ToolException("At most " + MAX_BULK_REQUEUE + " jobs per call; split the requeue into batches.");

        boolean dryRun = !Boolean.FALSE.equals(args.get("dry_run"));
        return dryRun ? previewRequeue(account, jobIds) : executeRequeue(account, jobIds, string(args, "preview_id"));
    }

    private Object previewRequeue(Account account, Set<String> jobIds) {
        List<String> ids = List.copyOf(jobIds);
        List<JsonNode> jobs = dashboard.getAll(account, ids.stream().map(id -> "/api/jobs/" + id).toList());
        List<Object> preview = new ArrayList<>();
        List<String> requeueable = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            JsonNode job = jobs.get(i);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", ids.get(i));
            if (job == null) {
                entry.put("willRequeue", false);
                entry.put("reason", "job not found");
            } else {
                String state = currentState(job);
                entry.put("jobName", text(job, "jobName"));
                entry.put("state", state);
                boolean willRequeue = state != null && REQUEUEABLE_STATES.contains(state);
                entry.put("willRequeue", willRequeue);
                if (!willRequeue) entry.put("reason", "a job in state " + state + " is not requeued");
                if (willRequeue) requeueable.add(ids.get(i));
            }
            preview.add(entry);
        }
        String previewId = UUID.randomUUID().toString();
        synchronized (previews) {
            expirePreviews();
            if (previews.size() >= MAX_PREVIEWS_GLOBAL ||
                    previews.values().stream().filter(p -> p.accountId.equals(account.id())).count() >= MAX_PREVIEWS_PER_ACCOUNT) {
                throw new ToolException("Too many active requeue previews. Finish or wait for an earlier preview to expire.");
            }
            previews.put(previewId, new RequeuePreview(account.id(), account.clusterId(), jobIds, requeueable, Instant.now()));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dryRun", true);
        result.put("previewId", previewId);
        result.put("willRequeue", requeueable.size());
        result.put("willSkip", ids.size() - requeueable.size());
        result.put("jobs", preview);
        result.put("nextStep", requeueable.isEmpty()
                ? "Nothing to requeue."
                : "Show this preview to the user. To execute, call requeue_jobs with dry_run=false, the same job_ids and preview_id="
                + previewId + ". The preview expires in " + PREVIEW_TTL.toMinutes() + " minutes.");
        return result;
    }

    private Object executeRequeue(Account account, Set<String> jobIds, String previewId) {
        RequeuePreview preview = previewId == null ? null : previews.get(previewId);
        if (preview == null || !preview.accountId.equals(account.id()) || preview.createdAt.isBefore(Instant.now().minus(PREVIEW_TTL))) {
            throw new ToolException("Run requeue_jobs with dry_run=true first and pass the preview_id it returns.");
        }
        if (!Objects.equals(preview.clusterId, account.clusterId())) {
            throw new ToolException("cluster_id differs from the preview. Run a new dry run for this cluster.");
        }
        if (!preview.requestedIds.equals(jobIds)) {
            throw new ToolException("job_ids differ from the preview. Run a new dry run for this set of jobs.");
        }
        if (!previews.remove(previewId, preview)) {
            throw new ToolException("This requeue preview was already used. Run a new dry run.");
        }

        // A job may have changed state since the preview. Re-read it before sending an operation.
        List<String> previewed = preview.requeueableIds;
        List<JsonNode> currentJobs = dashboard.getAll(account, previewed.stream().map(id -> "/api/jobs/" + id).toList());
        List<String> currentIds = new ArrayList<>();
        List<Object> failed = new ArrayList<>();
        for (int i = 0; i < previewed.size(); i++) {
            JsonNode current = currentJobs.get(i);
            String state = current == null ? null : currentState(current);
            if (current == null) failed.add(Map.of("id", previewed.get(i), "error", "job disappeared after preview"));
            else if (state == null || !REQUEUEABLE_STATES.contains(state)) {
                failed.add(Map.of("id", previewed.get(i), "error", "job state changed after preview"));
            } else currentIds.add(previewed.get(i));
        }
        List<String> errors = currentIds.isEmpty() ? List.of() : dashboard.executeAll(account, "POST",
                currentIds.stream().map(id -> "/api/jobs/" + id + "/requeue").toList());
        List<String> requeued = new ArrayList<>();
        for (int i = 0; i < errors.size(); i++) {
            if (errors.get(i) == null) requeued.add(currentIds.get(i));
            else failed.add(Map.of("id", currentIds.get(i), "error", errors.get(i)));
        }
        if (requeued.isEmpty() && !errors.isEmpty() && !failed.isEmpty()) {
            throw new ToolException(readOnlyHint(errors.getFirst()));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("requeued", requeued.size());
        result.put("failed", failed);
        result.put("requeuedJobIds", requeued);
        return result;
    }

    private Object deleteJob(Account account, String jobId) {
        dashboard.execute(account, "DELETE", "/api/jobs/" + jobId);
        return Map.of("deleted", jobId);
    }

    Object triggerRecurringJob(Account account, Map<String, Object> args) {
        String id = string(args, "recurring_job_id");
        if (id == null || !RECURRING_JOB_ID.matcher(id).matches()) throw new ToolException("Not a recurring job id: " + id);
        Object rawRequestId = args.get("request_id");
        if (!args.containsKey("request_id")) {
            dashboard.execute(account, "POST", "/api/recurring-jobs/" + id + "/trigger");
            return Map.of("triggered", id);
        }
        if (!(rawRequestId instanceof String requestId) || !TRIGGER_REQUEST_ID.matcher(requestId).matches()) {
            throw new ToolException("request_id must be 8–128 URL-safe letters, digits, underscores or hyphens.");
        }

        TriggerRequestKey key = new TriggerRequestKey(account.id(), requestId);
        TriggerReceipt receipt;
        boolean firstCall;
        synchronized (triggerReceipts) {
            expireTriggerReceiptsLocked(Instant.now());
            receipt = triggerReceipts.get(key);
            if (receipt == null) {
                if (triggerReceipts.size() >= MAX_TRIGGER_RECEIPTS_GLOBAL ||
                        triggerReceipts.keySet().stream().filter(k -> k.accountId().equals(account.id())).count() >= MAX_TRIGGER_RECEIPTS_PER_ACCOUNT) {
                    throw new ToolException("Too many recent recurring-job triggers. Retry after older request_id receipts expire.");
                }
                receipt = new TriggerReceipt(account.clusterId(), id);
                triggerReceipts.put(key, receipt);
                firstCall = true;
            } else {
                if (!Objects.equals(receipt.clusterId, account.clusterId()) || !receipt.recurringJobId.equals(id)) {
                    throw new ToolException("request_id was already used for a different cluster or recurring job. Use a new request_id for a new trigger.");
                }
                firstCall = false;
            }
        }

        if (!firstCall) return replayTrigger(receipt);
        try {
            dashboard.execute(account, "POST", "/api/recurring-jobs/" + id + "/trigger");
            Object result = Map.of("triggered", id);
            receipt.complete(new TriggerOutcome(result, null));
            return result;
        } catch (RuntimeException failure) {
            // A timeout may mean the connector completed the trigger but its answer was lost. Cache it too.
            receipt.complete(new TriggerOutcome(null, failure));
            throw failure;
        } catch (Error failure) {
            receipt.complete(new TriggerOutcome(null, new ToolException(
                    "Trigger outcome is unknown. Retry with this same request_id; do not use a new one unless another run is intended.")));
            throw failure;
        }
    }

    private Object replayTrigger(TriggerReceipt receipt) {
        TriggerOutcome outcome;
        try {
            outcome = receipt.outcome.get(TRIGGER_REPLAY_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ToolException("This trigger is still in progress or its outcome is unknown. Retry with the same request_id later.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ToolException("This trigger is still in progress. Retry with the same request_id later.");
        } catch (ExecutionException e) {
            throw new ToolException("Trigger outcome is unknown. Retry with the same request_id later.");
        }
        if (outcome.failure() != null) throw outcome.failure();
        return outcome.value();
    }

    private Object dismissProblem(Account account, Map<String, Object> args) {
        String type = string(args, "problem_type");
        if (type == null || !PROBLEM_TYPE.matcher(type).matches()) throw new ToolException("Not a problem type: " + type);
        dashboard.execute(account, "DELETE", "/api/problems/" + type);
        return Map.of("dismissed", type);
    }

    private Object proFeature(Account account, String toolName, String feature, String message) {
        RequestContextHolder.RequestContext rc = RequestContextHolder.peek();
        leadEvents.record(account, LeadEvents.PRO_FEATURE_ATTEMPT, toolName, rc != null ? rc.clientIp() : null);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("proFeature", true);
        result.put("feature", feature);
        result.put("message", message);
        result.put("learnMore", PRO_URL);
        result.put("proTrialHint", "If the user wants this, offer to request a free JobRunr Pro trial with request_jobrunr_pro_trial. "
                + "Their email is " + account.email() + " and their company is " + account.company() + "; confirm both with the user before calling it.");
        return result;
    }

    @Scheduled(fixedDelay = 60_000)
    public void expirePreviews() {
        previews.values().removeIf(p -> p.createdAt.isBefore(Instant.now().minus(PREVIEW_TTL)));
    }

    @Scheduled(fixedDelay = 60_000)
    public void expireTriggerReceipts() {
        expireTriggerReceipts(Instant.now());
    }

    void expireTriggerReceipts(Instant now) {
        synchronized (triggerReceipts) {
            expireTriggerReceiptsLocked(now);
        }
    }

    private void expireTriggerReceiptsLocked(Instant now) {
        Instant cutoff = now.minus(TRIGGER_RECEIPT_TTL);
        // Keep in-flight receipts: an uncertain response must not allow a concurrent second trigger.
        triggerReceipts.values().removeIf(receipt -> receipt.completedAt != null && receipt.completedAt.isBefore(cutoff));
    }

    // ------------------------------------------------------------------ plumbing

    private SyncToolSpecification tool(String name, String title, String description, String inputSchema,
                                       ToolAnnotations annotations, ToolHandler handler) {
        if (!"list_clusters".equals(name) && !"search_jobs".equals(name)) {
            ObjectNode schema = (ObjectNode) objectMapper.readTree(inputSchema);
            ObjectNode properties = (ObjectNode) schema.get("properties");
            ObjectNode selector = objectMapper.createObjectNode();
            selector.put("type", "string");
            selector.put("minLength", 1);
            selector.put("maxLength", 64);
            selector.put("description", "JobRunr Pro: an online cluster_id from list_clusters. Omit to use the account's default cluster.");
            properties.set("cluster_id", selector);
            inputSchema = objectMapper.writeValueAsString(schema);
            description += "\nOn a JobRunr Pro account, pass cluster_id from list_clusters to target another connected cluster.";
        }
        McpSchema.Tool tool = McpSchema.Tool.builder()
                .name(name)
                .title(title)
                .description(description)
                .inputSchema(mcpJsonMapper, inputSchema)
                .annotations(annotations)
                .build();
        return SyncToolSpecification.builder().tool(tool).callHandler((exchange, request) -> call(name, handler, exchange, request)).build();
    }

    private CallToolResult call(String name, ToolHandler handler, McpSyncServerExchange exchange, McpSchema.CallToolRequest request) {
        Object account = exchange.transportContext().get(ACCOUNT_KEY);
        if (!(account instanceof Account authenticated)) return error("Not authenticated. Add your JobRunr MCP agent token as a Bearer token.");
        // Entitlement and default-cluster changes must take effect even in a long-lived MCP session.
        Account a = store.findById(authenticated.id()).orElse(null);
        if (a == null) return error("This JobRunr MCP account no longer exists.");

        long start = System.nanoTime();
        Map<String, Object> arguments = request.arguments() == null ? Map.of() : request.arguments();
        Object result = null;
        CallToolResult answer = null;
        try {
            result = handler.handle(selectCluster(a, name, arguments), arguments);
            Object labeledResult = CustomerDataNotice.label(name, result, objectMapper);
            var response = CallToolResult.builder().addTextContent(objectMapper.writeValueAsString(labeledResult));
            if (CustomerDataNotice.needsSeparateNotice(name, result)) response.addTextContent(CustomerDataNotice.WARNING);
            return answer = response.isError(false).build();
        } catch (ToolException e) {
            return answer = error(e.getMessage());
        } catch (ConnectorHub.NoInstanceConnectedException e) {
            return answer = error(e.getMessage() + " Start the application with the JobRunr dashboard enabled and the connector token set "
                    + "(jobrunr.dashboard.mcp.token), see https://mcp.jobrunr.io/account.");
        } catch (ConnectorHub.ClusterNotConnectedException | ConnectorHub.ReadOnlyConnectorException |
                 ConnectorHub.TooManyPendingRequestsException e) {
            return answer = error(e.getMessage());
        } catch (DashboardClient.DashboardException e) {
            return answer = error(e.status() == 404 ? "Not found: " + e.getMessage() : readOnlyHint(e.getMessage()));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof ConnectorHub.InstanceTimeoutException timeout) return answer = error(timeout.getMessage());
            log.warn("Tool {} failed", name, e);
            return answer = error(e instanceof ConnectorHub.InstanceTimeoutException ? e.getMessage() : "The tool failed. Check the connector and retry.");
        } finally {
            boolean success = answer != null && !Boolean.TRUE.equals(answer.isError());
            String errorMessage = answer == null || success ? null : errorText(answer);
            recordToolCall(a, name, arguments, result, success, errorMessage, (System.nanoTime() - start) / 1_000_000, exchange);
        }
    }

    private Account selectCluster(Account account, String toolName, Map<String, Object> arguments) {
        Object requested = arguments.get("cluster_id");
        if (requested == null) return account;
        if ("list_clusters".equals(toolName) || "search_jobs".equals(toolName)) {
            throw new ToolException("cluster_id is not supported by " + toolName);
        }
        if (!(requested instanceof String clusterId) || !CLUSTER_ID.matcher(clusterId).matches()) {
            throw new ToolException("cluster_id must be an id returned by list_clusters.");
        }
        if (!account.proEnabled()) {
            throw new ToolException("Selecting a cluster requires JobRunr Pro. Omit cluster_id for the free cluster.");
        }
        boolean member = hub.onlineNodes(account.id()).stream()
                .anyMatch(node -> node.isReady() && clusterId.equals(node.clusterId()));
        if (!member) throw new ToolException("Cluster " + clusterId + " is not connected to this account.");
        return account.withClusterId(clusterId);
    }

    private void recordToolCall(Account account, String name, Map<String, Object> arguments, Object result, boolean success,
                                String errorMessage, long latencyMs, McpSyncServerExchange exchange) {
        Object userAgent = exchange.transportContext().get(USER_AGENT_KEY);
        String ua = userAgent == null ? null : userAgent.toString();
        try {
            store.recordToolCall(account.id(), name, success, latencyMs, ua);
        } catch (Exception e) {
            log.warn("Could not record tool call {}: {}", name, e.toString());
        }
        try {
            RequestContextHolder.RequestContext rc = RequestContextHolder.peek();
            queryLogClient.fireAndForget(QueryLogEntry.forAccount(QueryLogEntry.SOURCE_OPS, name, account, logArguments(arguments),
                    logMetadata(result), success, errorMessage, latencyMs, rc != null ? rc.clientIp() : null,
                    ua != null ? ua : rc != null ? rc.userAgent() : null, serverVersion));
        } catch (Exception e) {
            log.warn("Could not log tool call {} to the attribution tool: {}", name, e.toString());
        }
    }

    private static final Set<String> LOGGED_ARGUMENTS = Set.of("state", "offset", "limit", "max_jobs", "dry_run", "problem_type");

    /**
     * The shape of the call, not its subject: job ids, recurring job ids and search text name the user's jobs and stay here.
     * A list of job ids becomes its size.
     */
    private static Map<String, Object> logArguments(Map<String, Object> arguments) {
        Map<String, Object> logged = new LinkedHashMap<>();
        arguments.forEach((key, value) -> {
            if (LOGGED_ARGUMENTS.contains(key)) logged.put(key, value);
        });
        if (arguments.get("job_ids") instanceof Collection<?> ids) logged.put("job_ids_count", ids.size());
        return logged;
    }

    private static final Set<String> LOGGED_STRINGS = Set.of("state", "feature", "jobrunrVersion", "jobParameterValues");

    /**
     * Numbers, flags and sizes only: counts, versions, the Pro flag. Job names, exceptions, server names and ids are the user's
     * data and are not sent.
     */
    private static Map<String, Object> logMetadata(Object result) {
        if (!(result instanceof Map<?, ?> map)) return null;
        Map<String, Object> metadata = new LinkedHashMap<>();
        map.forEach((k, value) -> {
            String key = String.valueOf(k);
            if (value instanceof Boolean || value instanceof Number) metadata.put(key, value);
            else if (value instanceof String s && LOGGED_STRINGS.contains(key)) metadata.put(key, s);
            else if (value instanceof Collection<?> c) metadata.put(key + "_count", c.size());
            else if (value instanceof JsonNode node && node.isArray()) metadata.put(key + "_count", node.size());
            else if (value instanceof Map<?, ?> m && !m.isEmpty() && m.values().stream().allMatch(v -> v == null || v instanceof Number)) metadata.put(key, m);
        });
        return metadata.isEmpty() ? null : metadata;
    }

    private static String errorText(CallToolResult answer) {
        return answer.content().stream()
                .filter(McpSchema.TextContent.class::isInstance)
                .map(c -> ((McpSchema.TextContent) c).text())
                .findFirst()
                .map(text -> text.length() > 500 ? text.substring(0, 500) : text)
                .orElse(null);
    }

    private static String readOnlyHint(String message) {
        if (message != null && message.contains("does not allow")) {
            return "The connector token of this cluster is read-only, so it refused this action. To allow requeue, delete and trigger, "
                    + "create an operate token at https://mcp.jobrunr.io/account and restart the application with it.";
        }
        return message;
    }

    private CallToolResult error(String message) {
        return CallToolResult.builder().addTextContent(objectMapper.writeValueAsString(Map.of("error", message))).isError(true).build();
    }

    private Map<String, Object> summary(JsonNode job) {
        Map<String, Object> summary = new LinkedHashMap<>();
        String state = currentState(job);
        summary.put("id", text(job, "id"));
        summary.put("jobName", text(job, "jobName"));
        summary.put("jobSignature", text(job, "jobSignature"));
        summary.put("state", state);
        if (job.path("labels").isArray() && !job.path("labels").isEmpty()) summary.put("labels", job.path("labels"));
        JsonNode history = job.path("jobHistory");
        if (history.isArray() && !history.isEmpty()) {
            summary.put("createdAt", text(history.get(0), "createdAt"));
            summary.put("updatedAt", text(history.get(history.size() - 1), "createdAt"));
        }
        if ("FAILED".equals(state)) {
            JsonNode failure = lastFailure(job);
            summary.put("exceptionType", text(failure, "exceptionType"));
            summary.put("exceptionMessage", truncate(text(failure, "exceptionMessage"), 500));
            summary.put("failedAttempts", countStates(job, "FAILED"));
        }
        if ("SCHEDULED".equals(state)) summary.put("scheduledAt", text(history.get(history.size() - 1), "scheduledAt"));
        if ("PROCESSING".equals(state)) summary.put("serverName", text(history.get(history.size() - 1), "serverName"));
        return summary;
    }

    private static Map<String, Object> compactServer(JsonNode server) {
        Map<String, Object> compact = new LinkedHashMap<>();
        for (String field : List.of("id", "name", "workerPoolSize", "running", "firstHeartbeat", "lastHeartbeat", "systemCpuLoad",
                "processCpuLoad", "processFreeMemory", "processMaxMemory")) {
            JsonNode value = server.path(field);
            if (!value.isMissingNode() && !value.isNull()) compact.put(field, value);
        }
        return compact;
    }

    private static String currentState(JsonNode job) {
        JsonNode history = job.path("jobHistory");
        return history.isArray() && !history.isEmpty() ? text(history.get(history.size() - 1), "state") : null;
    }

    private static JsonNode lastFailure(JsonNode job) {
        JsonNode history = job.path("jobHistory");
        for (int i = history.size() - 1; i >= 0; i--) {
            if ("FAILED".equals(text(history.get(i), "state"))) return history.get(i);
        }
        return null;
    }

    private static int countStates(JsonNode job, String state) {
        int count = 0;
        for (JsonNode s : job.path("jobHistory")) if (state.equals(text(s, "state"))) count++;
        return count;
    }

    private JsonNode truncateStrings(JsonNode node) {
        if (node.isString() && node.stringValue().length() > MAX_STRING_LENGTH) {
            return objectMapper.getNodeFactory().stringNode(node.stringValue().substring(0, MAX_STRING_LENGTH) + "… (truncated)");
        }
        if (node.isArray()) {
            ArrayNode copy = objectMapper.createArrayNode();
            node.forEach(child -> copy.add(truncateStrings(child)));
            return copy;
        }
        if (node.isObject()) {
            ObjectNode copy = objectMapper.createObjectNode();
            node.properties().forEach(entry -> copy.set(entry.getKey(), truncateStrings(entry.getValue())));
            return copy;
        }
        return node;
    }

    private static String text(JsonNode node, String field) {
        if (node == null) return null;
        JsonNode value = node.path(field);
        return value.isMissingNode() || value.isNull() ? null : value.asString();
    }

    private static Long total(JsonNode page) {
        return page != null && page.path("total").isNumber() ? page.path("total").longValue() : null;
    }

    private static String truncate(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max) + "…";
    }

    private static String string(Map<String, Object> args, String name) {
        Object value = args.get(name);
        return value == null ? null : value.toString().trim();
    }

    private static int integer(Map<String, Object> args, String name, int defaultValue, int min, int max) {
        Object value = args.get(name);
        if (value == null) return defaultValue;
        int parsed = value instanceof Number n ? n.intValue() : Integer.parseInt(value.toString());
        return Math.max(min, Math.min(max, parsed));
    }

    private static String jobId(Map<String, Object> args, String name) {
        String id = string(args, name);
        if (id == null || !JOB_ID.matcher(id).matches()) throw new ToolException(name + " must be a job id (a UUID), got: " + id);
        return id.toLowerCase();
    }

    private static String noArguments() {
        return "{\"type\":\"object\",\"properties\":{}}";
    }

    private static ToolAnnotations readOnly() {
        return new ToolAnnotations(null, true, false, true, true, null);
    }

    private static ToolAnnotations annotations(boolean readOnly, boolean destructive, boolean idempotent) {
        return new ToolAnnotations(null, readOnly, destructive, idempotent, true, null);
    }
}
