package io.jobrunr.docsmcp.hub.mcp;

import io.jobrunr.docsmcp.hub.account.Account;
import io.jobrunr.docsmcp.hub.account.AccountStore;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;

/**
 * One public MCP url, two servers behind it. Requests to {@code /mcp} without an agent token keep reaching the docs
 * server exactly as before. Requests with {@code Authorization: Bearer jra_…} are routed to the ops server, which has
 * the docs tools plus the tools that operate the account's JobRunr cluster.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public class AgentAuthWebFilter implements WebFilter {

    public static final String PUBLIC_ENDPOINT = "/mcp";
    public static final String OPS_ENDPOINT = "/mcp-ops";
    public static final String ACCOUNT_ATTRIBUTE = "io.jobrunr.docsmcp.hub.account";

    private final AccountStore store;

    public AgentAuthWebFilter(AccountStore store) {
        this.store = store;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (OPS_ENDPOINT.equals(path)) {
            // only reachable through /mcp with a valid agent token
            exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);
            return exchange.getResponse().setComplete();
        }
        String authorization = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (!PUBLIC_ENDPOINT.equals(path) || authorization == null || !authorization.startsWith("Bearer jra_")) {
            return chain.filter(exchange);
        }

        String token = authorization.substring("Bearer ".length()).trim();
        return Mono.fromCallable(() -> store.findActiveToken(token, AccountStore.KIND_AGENT).flatMap(owner -> store.findById(owner.accountId())))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(account -> account.isPresent() ? routeToOpsServer(exchange, chain, account.get()) : unauthorized(exchange));
    }

    private Mono<Void> routeToOpsServer(ServerWebExchange exchange, WebFilterChain chain, Account account) {
        ServerWebExchange routed = exchange.mutate().request(exchange.getRequest().mutate().path(OPS_ENDPOINT).build()).build();
        routed.getAttributes().put(ACCOUNT_ATTRIBUTE, account);
        return chain.filter(routed);
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = "{\"error\":\"Unknown or revoked JobRunr MCP agent token. Get a new one at https://mcp.jobrunr.io/account\"}"
                .getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}
