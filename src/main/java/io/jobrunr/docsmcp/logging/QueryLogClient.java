package io.jobrunr.docsmcp.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class QueryLogClient {

    private static final Logger log = LoggerFactory.getLogger(QueryLogClient.class);

    private final WebClient webClient;
    private final String url;
    private final String secret;
    private final Duration timeout;
    private final boolean enabled;

    public QueryLogClient(
            @Value("${log.api.url:}") String url,
            @Value("${log.api.secret:}") String secret,
            @Value("${log.api.timeout:PT5S}") Duration timeout) {
        this.url = url;
        this.secret = secret;
        this.timeout = timeout;
        this.enabled = url != null && !url.isBlank() && secret != null && !secret.isBlank();
        this.webClient = WebClient.builder().build();
        if (!enabled) {
            log.info("QueryLogClient disabled (url or secret blank); MCP queries will not be logged remotely.");
        }
    }

    public void fireAndForget(QueryLogEntry entry) {
        if (!enabled || entry == null) return;

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("tool_name", entry.toolName());
        body.put("arguments", entry.arguments());
        body.put("result_metadata", entry.resultMetadata());
        body.put("result_body_truncated", entry.resultBodyTruncated());
        body.put("result_body_total_bytes", entry.resultBodyTotalBytes());
        body.put("success", entry.success());
        body.put("error_message", entry.errorMessage());
        body.put("latency_ms", entry.latencyMs());
        body.put("client_ip", entry.clientIp());
        body.put("user_agent", entry.userAgent());
        body.put("mcp_server_version", entry.mcpServerVersion());
        body.put("occurred_at", entry.occurredAt() != null ? entry.occurredAt().toString() : null);
        body.put("source", entry.source());
        if (entry.accountId() != null) {
            Map<String, Object> account = new LinkedHashMap<>();
            account.put("id", entry.accountId());
            account.put("email_domain", entry.accountEmailDomain());
            account.put("company", entry.accountCompany());
            account.put("cluster_id", entry.clusterId());
            body.put("account", account);
        }

        webClient.post()
                .uri(url)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + secret)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .retrieve()
                .toBodilessEntity()
                .timeout(timeout)
                .doOnError(e -> log.warn("MCP query log POST failed: {}", e.toString()))
                .onErrorResume(e -> Mono.empty())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }
}
