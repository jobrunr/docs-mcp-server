package io.jobrunr.docsmcp.hub.mcp;

import io.jobrunr.docsmcp.hub.account.Account;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The SDK has a global session cap but no per-account quota. Reserve a small share
 * for each authenticated account so one agent token cannot occupy every ops slot.
 * This runs after AgentAuthWebFilter routes a validated token to /mcp-ops.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 30)
public class OpsSessionQuotaWebFilter implements WebFilter {

    private static final String SESSION_HEADER = "Mcp-Session-Id";

    private record SessionOwner(String accountId, Instant lastSeenAt) {}

    private final Map<String, SessionOwner> owners = new HashMap<>();
    private final Map<String, Integer> reservations = new HashMap<>();
    private final long maxGlobal;
    private final int maxPerAccount;
    private final Duration ownerTtl;
    private int globalReservations;

    public OpsSessionQuotaWebFilter(
            @Value("${mcp.sessions.max:2000}") long maxGlobal,
            @Value("${mcp.sessions.max-per-account:20}") int maxPerAccount,
            @Value("${mcp.sessions.idle-timeout:PT30M}") Duration idleTimeout) {
        this.maxGlobal = maxGlobal;
        this.maxPerAccount = maxPerAccount;
        // The SDK checks for idle sessions once per timeout interval. Its eviction can
        // therefore occur up to two intervals after the last request.
        this.ownerTtl = idleTimeout.multipliedBy(2).plusSeconds(1);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!AgentAuthWebFilter.isInternalOpsPath(exchange.getRequest().getPath().value())) {
            return chain.filter(exchange);
        }
        Account account = exchange.getAttribute(AgentAuthWebFilter.ACCOUNT_ATTRIBUTE);
        if (account == null) return reject(exchange, HttpStatus.NOT_FOUND, "Unknown MCP endpoint");
        String accountId = account.id();
        String sessionId = exchange.getRequest().getHeaders().getFirst(SESSION_HEADER);

        if (HttpMethod.POST.equals(exchange.getRequest().getMethod()) && sessionId == null) {
            HttpStatus refused = reserve(accountId);
            if (refused != null) return reject(exchange, refused, "MCP session limit reached");
            return chain.filter(exchange)
                    .doFinally(signal -> finishInitialization(exchange, accountId));
        }
        if (sessionId != null) {
            if (!ownedBy(sessionId, accountId, exchange.getRequest().getMethod())) {
                return reject(exchange, HttpStatus.NOT_FOUND, "MCP session not found");
            }
            return chain.filter(exchange)
                    .doFinally(signal -> finishExistingRequest(exchange, sessionId, accountId));
        }
        return chain.filter(exchange);
    }

    private HttpStatus reserve(String accountId) {
        synchronized (owners) {
            purgeExpired();
            long active = owners.values().stream().filter(owner -> owner.accountId.equals(accountId)).count();
            if (active + reservations.getOrDefault(accountId, 0) >= maxPerAccount) {
                return HttpStatus.TOO_MANY_REQUESTS;
            }
            if (owners.size() + globalReservations >= maxGlobal) {
                return HttpStatus.SERVICE_UNAVAILABLE;
            }
            reservations.merge(accountId, 1, Integer::sum);
            globalReservations++;
            return null;
        }
    }

    private boolean ownedBy(String sessionId, String accountId, HttpMethod method) {
        synchronized (owners) {
            purgeExpired();
            SessionOwner owner = owners.get(sessionId);
            if (owner == null || !owner.accountId.equals(accountId)) return false;
            // Keep a long-running tool call allocated from its start, even before
            // the transport sends its response. Unsupported HTTP methods do not
            // reach the SDK and must not refresh an abandoned session.
            if (isSessionMethod(method)) owners.put(sessionId, new SessionOwner(accountId, Instant.now()));
            return true;
        }
    }

    private void finishInitialization(ServerWebExchange exchange, String accountId) {
        String assigned = exchange.getResponse().getHeaders().getFirst(SESSION_HEADER);
        synchronized (owners) {
            reservations.computeIfPresent(accountId, (key, count) -> count == 1 ? null : count - 1);
            globalReservations--;
            if (assigned != null && exchange.getResponse().getStatusCode() != null &&
                    exchange.getResponse().getStatusCode().is2xxSuccessful()) {
                owners.put(assigned, new SessionOwner(accountId, Instant.now()));
            }
        }
    }

    private void finishExistingRequest(ServerWebExchange exchange, String sessionId, String accountId) {
        synchronized (owners) {
            HttpMethod method = exchange.getRequest().getMethod();
            var status = exchange.getResponse().getStatusCode();
            boolean successfulDelete = HttpMethod.DELETE.equals(method) && status != null && status.is2xxSuccessful();
            // The SDK's GET, POST and DELETE routes return 404 only when this
            // session is absent. A 404 for PUT/PATCH comes from the router while
            // the SDK session may still be live, so it cannot free a quota slot.
            boolean sdkSessionMissing = isSessionMethod(method) && HttpStatus.NOT_FOUND.equals(status);
            if (successfulDelete || sdkSessionMissing) {
                owners.remove(sessionId);
            } else if (isSessionMethod(method) && status != null && status.is2xxSuccessful()) {
                owners.computeIfPresent(sessionId, (key, current) ->
                        current.accountId.equals(accountId) ? new SessionOwner(accountId, Instant.now()) : current);
            }
        }
    }

    private static boolean isSessionMethod(HttpMethod method) {
        return HttpMethod.GET.equals(method) || HttpMethod.POST.equals(method) || HttpMethod.DELETE.equals(method);
    }

    @Scheduled(fixedDelay = 60_000)
    public void purgeIdleOwners() {
        synchronized (owners) {
            purgeExpired();
        }
    }

    /** Close old SDK sessions after token rotation, then free the account's quota slots. */
    public void revokeAccountSessions(String accountId, Predicate<String> deleteSession) {
        List<String> sessionIds;
        synchronized (owners) {
            sessionIds = owners.entrySet().stream()
                    .filter(entry -> entry.getValue().accountId.equals(accountId))
                    .map(Map.Entry::getKey).toList();
        }
        for (String sessionId : sessionIds) {
            if (deleteSession.test(sessionId)) {
                synchronized (owners) {
                    SessionOwner owner = owners.get(sessionId);
                    if (owner != null && owner.accountId.equals(accountId)) owners.remove(sessionId);
                }
            }
        }
    }

    private void purgeExpired() {
        Instant cutoff = Instant.now().minus(ownerTtl);
        Iterator<SessionOwner> iterator = owners.values().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().lastSeenAt.isBefore(cutoff)) iterator.remove();
        }
    }

    private static Mono<Void> reject(ServerWebExchange exchange, HttpStatus status, String message) {
        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        if (status == HttpStatus.TOO_MANY_REQUESTS) {
            exchange.getResponse().getHeaders().set("Retry-After", "60");
        }
        byte[] body = ("{\"error\":\"" + message + "\"}").getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}
