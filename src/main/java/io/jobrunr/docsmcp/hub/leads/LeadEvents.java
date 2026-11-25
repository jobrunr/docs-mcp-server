package io.jobrunr.docsmcp.hub.leads;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import io.jobrunr.docsmcp.logging.QueryLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The sales side of the hub. Every signal lands in {@code hub_lead_events}; when {@code hub.leads-webhook-url} is set it
 * is also posted to n8n, which creates or updates the HubSpot contact (form {@code mcp-ops-*}, like the trial tool's
 * {@code mcp-trial}), and logged to the attribution tool as tool {@code hub_<event>} with the account's email domain.
 */
@Component
public class LeadEvents {

    public static final String VERIFIED = "verified";
    public static final String FIRST_CONNECTION = "first_connection";
    public static final String PRO_FEATURE_ATTEMPT = "pro_feature_attempt";
    public static final String SECOND_CLUSTER_ATTEMPT = "second_cluster_attempt";

    private static final Logger log = LoggerFactory.getLogger(LeadEvents.class);
    private static final Duration REPEAT_EVENT_COOLDOWN = Duration.ofHours(1);
    private static final int MAX_EVENT_KEYS = 10_000;

    private final AccountStore store;
    private final QueryLogClient queryLogClient;
    private final String serverVersion;
    private final WebClient webClient = WebClient.builder().build();
    private final String webhookUrl;
    private final Map<EventKey, Instant> lastRepeatEvent = new LinkedHashMap<>(128, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<EventKey, Instant> eldest) {
            return size() > MAX_EVENT_KEYS;
        }
    };

    private record EventKey(String accountId, String event, String detail) {}

    public LeadEvents(AccountStore store, HubProperties properties, QueryLogClient queryLogClient,
                      @Value("${spring.ai.mcp.server.version:unknown}") String serverVersion) {
        this.store = store;
        this.queryLogClient = queryLogClient;
        this.serverVersion = serverVersion;
        this.webhookUrl = properties.leadsWebhookUrl();
    }

    public void record(Account account, String event, String detail) {
        record(account, event, detail, null);
    }

    /** @param clientIp the browser or agent that caused the event, when there is one; never the user's application */
    public void record(Account account, String event, String detail, String clientIp) {
        EventKey repeatKey = repeatKey(account.id(), event, detail);
        if (repeatKey != null && !reserveRepeatEvent(repeatKey)) return;
        try {
            store.recordLeadEvent(account.id(), event, detail);
        } catch (RuntimeException e) {
            if (repeatKey != null) releaseRepeatEvent(repeatKey);
            throw e;
        }
        log.info("Lead event {} for {} ({})", event, account.emailDomain(), detail);
        logToAttribution(account, event, detail, clientIp);
        if (webhookUrl == null || webhookUrl.isBlank()) return;

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("form", "mcp-ops-" + event.replace('_', '-'));
        body.put("email", account.email());
        body.put("name", account.name());
        body.put("company", account.company());
        body.put("role", account.role());
        body.put("use_case", account.useCase());
        body.put("detail", detail);
        body.put("source", "mcp-ops");
        body.put("submitted_at", Instant.now().toString());
        webClient.post().uri(webhookUrl).bodyValue(body)
                .retrieve().toBodilessEntity()
                .timeout(Duration.ofSeconds(10))
                .doOnError(e -> log.warn("Lead webhook failed for event {}: {}", event, e.toString()))
                .onErrorResume(e -> Mono.empty())
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private static EventKey repeatKey(String accountId, String event, String detail) {
        if (SECOND_CLUSTER_ATTEMPT.equals(event)) return new EventKey(accountId, event, null);
        if (PRO_FEATURE_ATTEMPT.equals(event)) return new EventKey(accountId, event, detail);
        return null;
    }

    private synchronized boolean reserveRepeatEvent(EventKey key) {
        Instant now = Instant.now();
        Instant last = lastRepeatEvent.get(key);
        if (last != null && last.isAfter(now.minus(REPEAT_EVENT_COOLDOWN))) return false;
        lastRepeatEvent.put(key, now);
        return true;
    }

    private synchronized void releaseRepeatEvent(EventKey key) {
        lastRepeatEvent.remove(key);
    }

    private void logToAttribution(Account account, String event, String detail, String clientIp) {
        try {
            Map<String, Object> arguments = new LinkedHashMap<>();
            arguments.put("detail", detail != null && detail.length() > 200 ? detail.substring(0, 200) : detail);
            if (VERIFIED.equals(event)) arguments.put("role", account.role());
            queryLogClient.fireAndForget(QueryLogEntry.forAccount(QueryLogEntry.SOURCE_HUB, "hub_" + event, account, arguments,
                    null, true, null, 0, clientIp, null, serverVersion));
        } catch (Exception e) {
            log.warn("Could not log lead event {} to the attribution tool: {}", event, e.toString());
        }
    }
}
