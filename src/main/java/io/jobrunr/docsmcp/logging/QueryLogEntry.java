package io.jobrunr.docsmcp.logging;

import io.jobrunr.docsmcp.hub.account.Account;

import java.time.Instant;
import java.util.Map;

/**
 * One row for the attribution tool's {@code mcp_query_log}.
 *
 * @param source "docs" for the public docs tools, "ops" for the cluster tools behind an agent token, "hub" for a hub lead
 *               event (sign-up verified, first connection, ...). Docs tools called through the ops server still say "docs":
 *               they share the docs callbacks and see no account.
 * @param accountId          the hub account, ops and hub rows only
 * @param accountEmailDomain the domain of the account's email, never the full address
 * @param accountCompany     the company the user typed at sign-up
 * @param clusterId          the JobRunr cluster id linked to the account, when one connected
 */
public record QueryLogEntry(
        String toolName,
        Map<String, Object> arguments,
        Map<String, Object> resultMetadata,
        String resultBodyTruncated,
        Integer resultBodyTotalBytes,
        boolean success,
        String errorMessage,
        long latencyMs,
        String clientIp,
        String userAgent,
        String mcpServerVersion,
        Instant occurredAt,
        String source,
        String accountId,
        String accountEmailDomain,
        String accountCompany,
        String clusterId
) {

    public static final String SOURCE_DOCS = "docs";
    public static final String SOURCE_OPS = "ops";
    public static final String SOURCE_HUB = "hub";

    /** A docs tool call: no account. */
    public QueryLogEntry(String toolName, Map<String, Object> arguments, Map<String, Object> resultMetadata,
                         String resultBodyTruncated, Integer resultBodyTotalBytes, boolean success, String errorMessage,
                         long latencyMs, String clientIp, String userAgent, String mcpServerVersion, Instant occurredAt) {
        this(toolName, arguments, resultMetadata, resultBodyTruncated, resultBodyTotalBytes, success, errorMessage, latencyMs,
                clientIp, userAgent, mcpServerVersion, occurredAt, SOURCE_DOCS, null, null, null, null);
    }

    /** An ops tool call or hub event made by a known account. No result body: it would carry the user's job data. */
    public static QueryLogEntry forAccount(String source, String toolName, Account account, Map<String, Object> arguments,
                                           Map<String, Object> resultMetadata, boolean success, String errorMessage,
                                           long latencyMs, String clientIp, String userAgent, String mcpServerVersion) {
        return new QueryLogEntry(toolName, arguments, resultMetadata, null, null, success, errorMessage, latencyMs,
                clientIp, userAgent, mcpServerVersion, Instant.now(), source,
                account.id(), account.emailDomain(), account.company(), account.clusterId());
    }
}
