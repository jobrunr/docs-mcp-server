package io.jobrunr.docsmcp.web;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class SecurityHeadersWebFilterTest {

    @Test
    void httpsPublicEndpointSetsStrictSecurityHeaders() {
        var properties = new HubProperties("https://mcp.example.invalid", "JobRunr <noreply@example.invalid>",
                "", Duration.ofSeconds(25), Duration.ofSeconds(20), Duration.ofMinutes(30), Duration.ofDays(30));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://127.0.0.1/signup"));

        new SecurityHeadersWebFilter(properties).filter(exchange, ignored -> Mono.empty()).block();

        var headers = exchange.getResponse().getHeaders();
        assertThat(headers.getFirst("Content-Security-Policy")).contains("script-src 'self'", "frame-ancestors 'none'")
                .doesNotContain("script-src 'unsafe-inline'");
        assertThat(headers.getFirst("Strict-Transport-Security")).isEqualTo("max-age=31536000");
        assertThat(headers.getFirst("X-Content-Type-Options")).isEqualTo("nosniff");
        assertThat(headers.getFirst("Referrer-Policy")).isEqualTo("no-referrer");
    }

    @Test
    void localHttpEndpointDoesNotSetHsts() {
        var properties = new HubProperties("http://localhost:18080", "JobRunr <noreply@example.invalid>",
                "", Duration.ofSeconds(25), Duration.ofSeconds(20), Duration.ofMinutes(30), Duration.ofDays(30));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://127.0.0.1/signup"));

        new SecurityHeadersWebFilter(properties).filter(exchange, ignored -> Mono.empty()).block();

        assertThat(exchange.getResponse().getHeaders().getFirst("Strict-Transport-Security")).isNull();
    }
}
