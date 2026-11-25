package io.jobrunr.docsmcp.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitWebFilterTest {

    private static final WebFilterChain PASS = exchange -> Mono.empty();

    @Test
    void allowsUpToLimitPerIp() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 10);
        for (int i = 0; i < 10; i++) {
            MockServerWebExchange exchange = exchange("1.2.3.4", "/sse");
            StepVerifier.create(filter.filter(exchange, PASS)).verifyComplete();
            assertThat(exchange.getResponse().getStatusCode()).isNull();
        }
    }

    @Test
    void rejectsBeyondLimitWith429AndRetryAfter() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 3);
        for (int i = 0; i < 3; i++) {
            filter.filter(exchange("9.9.9.9", "/sse"), PASS).block();
        }
        MockServerWebExchange exchange = exchange("9.9.9.9", "/sse");
        filter.filter(exchange, PASS).block();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(exchange.getResponse().getHeaders().getFirst("Retry-After")).isEqualTo("60");
    }

    @Test
    void separateIpsHaveIndependentBuckets() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 2);
        filter.filter(exchange("1.1.1.1", "/sse"), PASS).block();
        filter.filter(exchange("1.1.1.1", "/sse"), PASS).block();

        MockServerWebExchange other = exchange("2.2.2.2", "/sse");
        filter.filter(other, PASS).block();
        assertThat(other.getResponse().getStatusCode()).isNull();
    }

    @Test
    void actuatorPathsBypassRateLimit() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 1);
        filter.filter(exchange("3.3.3.3", "/actuator/health"), PASS).block();
        MockServerWebExchange second = exchange("3.3.3.3", "/actuator/health");
        filter.filter(second, PASS).block();
        assertThat(second.getResponse().getStatusCode()).isNull();
    }

    @Test
    void disabledFlagShortCircuits() {
        RateLimitWebFilter filter = new RateLimitWebFilter(false, 1);
        for (int i = 0; i < 5; i++) {
            MockServerWebExchange e = exchange("4.4.4.4", "/sse");
            filter.filter(e, PASS).block();
            assertThat(e.getResponse().getStatusCode()).isNull();
        }
    }

    @Test
    void newIpsShareAnOverflowLimitAfterTheBucketCapWithoutResettingExistingIps() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 2, 2);
        filter.filter(exchange("1.1.1.1", "/mcp"), PASS).block();
        filter.filter(exchange("2.2.2.2", "/mcp"), PASS).block();
        filter.filter(exchange("3.3.3.3", "/mcp"), PASS).block();
        filter.filter(exchange("4.4.4.4", "/mcp"), PASS).block();

        MockServerWebExchange overflow = exchange("5.5.5.5", "/mcp");
        filter.filter(overflow, PASS).block();
        assertThat(overflow.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        filter.filter(exchange("1.1.1.1", "/mcp"), PASS).block();
        MockServerWebExchange original = exchange("1.1.1.1", "/mcp");
        filter.filter(original, PASS).block();
        assertThat(original.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void overlongUntrustedIpHeadersShareABoundedKey() {
        RateLimitWebFilter filter = new RateLimitWebFilter(true, 1, 2);
        String prefix = "x".repeat(64);
        filter.filter(exchange(prefix + "first", "/mcp"), PASS).block();

        MockServerWebExchange second = exchange(prefix + "second", "/mcp");
        filter.filter(second, PASS).block();
        assertThat(second.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    private MockServerWebExchange exchange(String flyClientIp, String path) {
        return MockServerWebExchange.from(MockServerHttpRequest
                .get(path)
                .header("Fly-Client-IP", flyClientIp));
    }
}
