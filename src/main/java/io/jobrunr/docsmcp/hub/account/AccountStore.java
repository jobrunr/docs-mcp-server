package io.jobrunr.docsmcp.hub.account;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * All persistence of the ops hub. Plain JDBC on purpose: a handful of tables, no ORM needed.
 */
@Repository
public class AccountStore {

    public static final String KIND_CONNECTOR = "connector";
    public static final String KIND_AGENT = "agent";

    private final JdbcClient jdbc;

    public AccountStore(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // ------------------------------------------------------------------ accounts

    public Optional<Account> findById(String id) {
        return jdbc.sql("SELECT * FROM hub_accounts WHERE id = ?").param(id).query(AccountStore::account).optional();
    }

    public Optional<Account> findByEmail(String email) {
        return jdbc.sql("SELECT * FROM hub_accounts WHERE email = ?").param(email.toLowerCase()).query(AccountStore::account).optional();
    }

    public Account create(String email, String name, String company, String role, String useCase) {
        String id = UUID.randomUUID().toString();
        jdbc.sql("INSERT INTO hub_accounts (id, email, name, company, job_role, use_case, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .params(id, email.toLowerCase(), name, company, role, useCase, ts(Instant.now()))
                .update();
        return findById(id).orElseThrow();
    }

    /** @return true if this call verified the account, false if it was verified before */
    public boolean markVerified(String accountId) {
        return jdbc.sql("UPDATE hub_accounts SET verified_at = ? WHERE id = ? AND verified_at IS NULL")
                .params(ts(Instant.now()), accountId)
                .update() == 1;
    }

    public void linkCluster(String accountId, String clusterId) {
        jdbc.sql("UPDATE hub_accounts SET cluster_id = ? WHERE id = ?").params(clusterId, accountId).update();
    }

    // ------------------------------------------------------------------ magic links and sessions

    public String createMagicLink(String accountId, Duration ttl) {
        String secret = Tokens.random(48);
        jdbc.sql("INSERT INTO hub_magic_links (token_hash, account_id, expires_at) VALUES (?, ?, ?)")
                .params(Tokens.hash(secret), accountId, ts(Instant.now().plus(ttl)))
                .update();
        return secret;
    }

    @Transactional
    public Optional<String> consumeMagicLink(String secret) {
        String hash = Tokens.hash(secret);
        int used = jdbc.sql("UPDATE hub_magic_links SET used_at = ? WHERE token_hash = ? AND used_at IS NULL AND expires_at > ?")
                .params(ts(Instant.now()), hash, ts(Instant.now()))
                .update();
        if (used != 1) return Optional.empty();
        return jdbc.sql("SELECT account_id FROM hub_magic_links WHERE token_hash = ?").param(hash).query(String.class).optional();
    }

    public String createSession(String accountId, Duration ttl) {
        String secret = Tokens.random(48);
        jdbc.sql("INSERT INTO hub_sessions (token_hash, account_id, expires_at) VALUES (?, ?, ?)")
                .params(Tokens.hash(secret), accountId, ts(Instant.now().plus(ttl)))
                .update();
        return secret;
    }

    public Optional<String> accountForSession(String secret) {
        return jdbc.sql("SELECT account_id FROM hub_sessions WHERE token_hash = ? AND expires_at > ?")
                .params(Tokens.hash(secret), ts(Instant.now()))
                .query(String.class)
                .optional();
    }

    public void deleteSession(String secret) {
        jdbc.sql("DELETE FROM hub_sessions WHERE token_hash = ?").param(Tokens.hash(secret)).update();
    }

    // ------------------------------------------------------------------ connector and agent tokens

    public record IssuedTokens(String connectorToken, String agentToken) {
    }

    public record TokenOwner(String accountId, String scope) {
    }

    public record TokenInfo(String kind, String scope, String hint, Instant createdAt) {
    }

    /** Revokes the current tokens of the account and issues a new connector and agent token. */
    @Transactional
    public IssuedTokens issueTokens(String accountId, boolean operate) {
        Instant now = Instant.now();
        jdbc.sql("UPDATE hub_tokens SET revoked_at = ? WHERE account_id = ? AND revoked_at IS NULL").params(ts(now), accountId).update();
        String connectorToken = Tokens.connectorToken(operate);
        String agentToken = Tokens.agentToken();
        insertToken(accountId, KIND_CONNECTOR, operate ? "operate" : "read", connectorToken, now);
        insertToken(accountId, KIND_AGENT, null, agentToken, now);
        return new IssuedTokens(connectorToken, agentToken);
    }

    private void insertToken(String accountId, String kind, String scope, String token, Instant now) {
        jdbc.sql("INSERT INTO hub_tokens (id, account_id, kind, scope, token_hash, hint, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .params(UUID.randomUUID().toString(), accountId, kind, scope, Tokens.hash(token), Tokens.hint(token), ts(now))
                .update();
    }

    public Optional<TokenOwner> findActiveToken(String token, String kind) {
        return jdbc.sql("SELECT account_id, scope FROM hub_tokens WHERE token_hash = ? AND kind = ? AND revoked_at IS NULL")
                .params(Tokens.hash(token), kind)
                .query((rs, i) -> new TokenOwner(rs.getString("account_id"), rs.getString("scope")))
                .optional();
    }

    public List<TokenInfo> activeTokens(String accountId) {
        return jdbc.sql("SELECT kind, scope, hint, created_at FROM hub_tokens WHERE account_id = ? AND revoked_at IS NULL ORDER BY kind")
                .param(accountId)
                .query((rs, i) -> new TokenInfo(rs.getString("kind"), rs.getString("scope"), rs.getString("hint"), instant(rs, "created_at")))
                .list();
    }

    // ------------------------------------------------------------------ instances

    public record InstanceSnapshot(
            String accountId, String clusterId, String jobrunrVersion, String javaVersion, String framework,
            String storageProvider, Boolean redaction, Integer servers, Long recurringJobs,
            Long jobsScheduled, Long jobsEnqueued, Long jobsProcessing, Long jobsFailed, Long jobsSucceeded,
            Instant firstConnectedAt, Instant snapshotAt) {
    }

    /** @return true if this is the first snapshot ever stored for the account */
    public boolean saveInstance(InstanceSnapshot s) {
        int updated = jdbc.sql("""
                        UPDATE hub_instances SET cluster_id = ?, jobrunr_version = ?, java_version = ?, framework = ?, storage_provider = ?,
                            redaction = ?, servers = ?, recurring_jobs = ?, jobs_scheduled = ?, jobs_enqueued = ?, jobs_processing = ?,
                            jobs_failed = ?, jobs_succeeded = ?, snapshot_at = ?
                        WHERE account_id = ?""")
                .params(s.clusterId(), s.jobrunrVersion(), s.javaVersion(), s.framework(), s.storageProvider(), s.redaction(), s.servers(),
                        s.recurringJobs(), s.jobsScheduled(), s.jobsEnqueued(), s.jobsProcessing(), s.jobsFailed(), s.jobsSucceeded(),
                        ts(s.snapshotAt()), s.accountId())
                .update();
        if (updated == 1) return false;
        jdbc.sql("""
                        INSERT INTO hub_instances (account_id, cluster_id, jobrunr_version, java_version, framework, storage_provider, redaction,
                            servers, recurring_jobs, jobs_scheduled, jobs_enqueued, jobs_processing, jobs_failed, jobs_succeeded,
                            first_connected_at, snapshot_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
                .params(s.accountId(), s.clusterId(), s.jobrunrVersion(), s.javaVersion(), s.framework(), s.storageProvider(), s.redaction(),
                        s.servers(), s.recurringJobs(), s.jobsScheduled(), s.jobsEnqueued(), s.jobsProcessing(), s.jobsFailed(), s.jobsSucceeded(),
                        ts(s.snapshotAt()), ts(s.snapshotAt()))
                .update();
        return true;
    }

    public Optional<InstanceSnapshot> findInstance(String accountId) {
        return jdbc.sql("SELECT * FROM hub_instances WHERE account_id = ?").param(accountId)
                .query((rs, i) -> new InstanceSnapshot(
                        rs.getString("account_id"), rs.getString("cluster_id"), rs.getString("jobrunr_version"), rs.getString("java_version"),
                        rs.getString("framework"), rs.getString("storage_provider"), (Boolean) rs.getObject("redaction"),
                        (Integer) rs.getObject("servers"), longOrNull(rs, "recurring_jobs"), longOrNull(rs, "jobs_scheduled"),
                        longOrNull(rs, "jobs_enqueued"), longOrNull(rs, "jobs_processing"), longOrNull(rs, "jobs_failed"),
                        longOrNull(rs, "jobs_succeeded"), instant(rs, "first_connected_at"), instant(rs, "snapshot_at")))
                .optional();
    }

    // ------------------------------------------------------------------ usage and lead events

    public record ToolCall(String toolName, boolean success, long latencyMs, Instant occurredAt) {
    }

    public void recordToolCall(String accountId, String toolName, boolean success, long latencyMs, String userAgent) {
        jdbc.sql("INSERT INTO hub_tool_calls (account_id, tool_name, success, latency_ms, user_agent, occurred_at) VALUES (?, ?, ?, ?, ?, ?)")
                .params(accountId, toolName, success, latencyMs, truncate(userAgent, 300), ts(Instant.now()))
                .update();
    }

    public List<ToolCall> recentToolCalls(String accountId, int limit) {
        return jdbc.sql("SELECT tool_name, success, latency_ms, occurred_at FROM hub_tool_calls WHERE account_id = ? ORDER BY occurred_at DESC, id DESC LIMIT " + limit)
                .param(accountId)
                .query((rs, i) -> new ToolCall(rs.getString("tool_name"), rs.getBoolean("success"), rs.getLong("latency_ms"), instant(rs, "occurred_at")))
                .list();
    }

    public void recordLeadEvent(String accountId, String event, String detail) {
        jdbc.sql("INSERT INTO hub_lead_events (account_id, event, detail, occurred_at) VALUES (?, ?, ?, ?)")
                .params(accountId, event, truncate(detail, 500), ts(Instant.now()))
                .update();
    }

    public boolean hasLeadEvent(String accountId, String event) {
        return jdbc.sql("SELECT COUNT(*) FROM hub_lead_events WHERE account_id = ? AND event = ?")
                .params(accountId, event).query(Long.class).single() > 0;
    }

    // ------------------------------------------------------------------ helpers

    private static Account account(ResultSet rs, int rowNum) throws SQLException {
        return new Account(rs.getString("id"), rs.getString("email"), rs.getString("name"), rs.getString("company"),
                rs.getString("job_role"), rs.getString("use_case"), rs.getString("cluster_id"),
                instant(rs, "created_at"), instant(rs, "verified_at"));
    }

    private static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Long longOrNull(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static String truncate(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }
}
