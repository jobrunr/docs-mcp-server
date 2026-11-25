package io.jobrunr.docsmcp.ratelimit;

import io.jobrunr.docsmcp.web.ClientIpExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RateLimitWebFilter implements WebFilter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitWebFilter.class);
    private static final long WINDOW_MS = 60_000L;
    private static final long IDLE_PURGE_MS = 5 * 60_000L;
    private static final int MAX_IP_BUCKETS = 10_000;
    private static final int MAX_IP_KEY_LENGTH = 64;
    private static final String UNKNOWN_IP = "_unknown_";
    private static final String BODY_TEMPLATE = "{\"error\":\"Rate limit exceeded\",\"limit\":%d,\"window\":\"1m\"}";

    private final Map<String, Deque<Long>> hits = new HashMap<>();
    private final Deque<Long> overflowHits = new ArrayDeque<>();
    private final boolean enabled;
    private final int requestsPerMinute;
    private final int maxIpBuckets;

    @Autowired
    public RateLimitWebFilter(
            @Value("${ratelimit.enabled:true}") boolean enabled,
            @Value("${ratelimit.requests-per-minute:10}") int requestsPerMinute) {
        this(enabled, requestsPerMinute, MAX_IP_BUCKETS);
    }

    RateLimitWebFilter(boolean enabled, int requestsPerMinute, int maxIpBuckets) {
        this.enabled = enabled;
        this.requestsPerMinute = requestsPerMinute;
        this.maxIpBuckets = maxIpBuckets;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!enabled) return chain.filter(exchange);

        String path = exchange.getRequest().getPath().value();
        if (path.startsWith("/actuator") || path.startsWith("/connector/")) {
            return chain.filter(exchange);
        }

        String ip = ClientIpExtractor.from(exchange.getRequest());
        String key = ip != null ? ip : UNKNOWN_IP;
        if (key.length() > MAX_IP_KEY_LENGTH) key = key.substring(0, MAX_IP_KEY_LENGTH);
        long now = System.currentTimeMillis();

        boolean allowed;
        synchronized (hits) {
            Deque<Long> deque = hits.get(key);
            if (deque == null) {
                // New addresses share one overflow bucket at capacity. Evicting existing buckets
                // would let a caller rotate apparent IPs to reset its own limit.
                if (hits.size() < maxIpBuckets) {
                    deque = new ArrayDeque<>();
                    hits.put(key, deque);
                } else {
                    deque = overflowHits;
                }
            }
            Iterator<Long> it = deque.iterator();
            while (it.hasNext()) {
                if (now - it.next() >= WINDOW_MS) it.remove();
                else break;
            }
            allowed = deque.size() < requestsPerMinute;
            if (allowed) deque.addLast(now);
        }

        return allowed ? chain.filter(exchange) : reject(exchange, key);
    }

    private Mono<Void> reject(ServerWebExchange exchange, String ip) {
        log.warn("Rate limit exceeded for {}", ip);
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().add(HttpHeaders.RETRY_AFTER, "60");
        byte[] body = String.format(BODY_TEMPLATE, requestsPerMinute).getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = response.bufferFactory().wrap(body);
        return response.writeWith(Mono.just(buffer));
    }

    @Scheduled(fixedDelay = 5 * 60_000L)
    public void purgeIdle() {
        long now = System.currentTimeMillis();
        synchronized (hits) {
            Iterator<Map.Entry<String, Deque<Long>>> entries = hits.entrySet().iterator();
            while (entries.hasNext()) {
                Deque<Long> dq = entries.next().getValue();
                Long last = dq.peekLast();
                if (last == null || now - last > IDLE_PURGE_MS) entries.remove();
            }
            Long lastOverflow = overflowHits.peekLast();
            if (lastOverflow == null || now - lastOverflow > IDLE_PURGE_MS) overflowHits.clear();
        }
    }
}
