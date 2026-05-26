package io.jobrunr.docsmcp.web;

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
public class SseKeepAliveFilter implements WebFilter {

    private static final String SSE_PATH = "/sse";
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(25);
    private static final byte[] HEARTBEAT_BYTES = ": keepalive\n\n".getBytes(StandardCharsets.UTF_8);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!SSE_PATH.equals(exchange.getRequest().getPath().value())) {
            return chain.filter(exchange);
        }

        ServerHttpResponse original = exchange.getResponse();
        DataBufferFactory bufferFactory = original.bufferFactory();

        ServerHttpResponseDecorator decorated = new ServerHttpResponseDecorator(original) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                Sinks.Empty<Void> stop = Sinks.empty();
                Flux<DataBuffer> bodyFlux = Flux.from(body)
                        .cast(DataBuffer.class)
                        .doOnComplete(stop::tryEmitEmpty)
                        .doOnError(e -> stop.tryEmitEmpty())
                        .doOnCancel(stop::tryEmitEmpty);
                Flux<DataBuffer> heartbeat = Flux.interval(HEARTBEAT_INTERVAL)
                        .map(i -> bufferFactory.wrap(HEARTBEAT_BYTES.clone()))
                        .takeUntilOther(stop.asMono())
                        .onErrorResume(e -> Flux.empty());
                return super.writeWith(Flux.merge(bodyFlux, heartbeat));
            }

            @Override
            public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
                return writeWith(Flux.from(body).flatMap(Flux::from));
            }
        };

        return chain.filter(exchange.mutate().response(decorated).build());
    }
}
