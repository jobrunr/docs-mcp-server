package io.jobrunr.docsmcp.hub.leads;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * {@code mcp-trial}).
 */
@Component
public class LeadEvents {

    public static final String VERIFIED = "verified";
    public static final String FIRST_CONNECTION = "first_connection";
    public static final String PRO_FEATURE_ATTEMPT = "pro_feature_attempt";
    public static final String SECOND_CLUSTER_ATTEMPT = "second_cluster_attempt";

    private static final Logger log = LoggerFactory.getLogger(LeadEvents.class);

    private final AccountStore store;
    private final WebClient webClient = WebClient.builder().build();
    private final String webhookUrl;

    public LeadEvents(AccountStore store, HubProperties properties) {
        this.store = store;
        this.webhookUrl = properties.leadsWebhookUrl();
    }

    public void record(Account account, String event, String detail) {
        store.recordLeadEvent(account.id(), event, detail);
        log.info("Lead event {} for {} ({})", event, account.emailDomain(), detail);
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
}
