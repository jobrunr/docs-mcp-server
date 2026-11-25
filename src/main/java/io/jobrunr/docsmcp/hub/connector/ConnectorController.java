package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.account.AccountStore;
import io.jobrunr.docsmcp.hub.account.Account;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

/**
 * The two endpoints the connector inside a customer's JVM talks to. Both authenticate with the connector token.
 */
@RestController
public class ConnectorController {

    private static final Pattern NODE_ID = Pattern.compile("[A-Za-z0-9-]{8,64}");
    private static final Pattern REQUEST_ID = Pattern.compile("[0-9a-f]{32}");
    private static final Pattern CONNECTOR_TOKEN = Pattern.compile("jrc_(?:read|operate)_[A-Za-z0-9]{40}");
    private static final int MAX_RESULT_BYTES = 4 * 1024 * 1024;
    private static final int MAX_CONCURRENT_AUTH_LOOKUPS = 128;

    private final ConnectorHub hub;
    private final InstanceMonitor monitor;
    private final AccountStore store;
    private final Semaphore authLookups = new Semaphore(MAX_CONCURRENT_AUTH_LOOKUPS);

    public ConnectorController(ConnectorHub hub, InstanceMonitor monitor, AccountStore store) {
        this.hub = hub;
        this.monitor = monitor;
        this.store = store;
    }

    @GetMapping("/connector/poll")
    public Mono<ResponseEntity<String>> poll(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestHeader(value = "X-JobRunr-Node", required = false) String nodeId,
            @RequestHeader(value = "X-JobRunr-Version", required = false) String jobrunrVersion,
            @RequestHeader(value = "X-JobRunr-Java-Version", required = false) String javaVersion,
            @RequestHeader(value = "X-JobRunr-Framework", required = false) String framework,
            @RequestHeader(value = "X-JobRunr-Redaction", required = false) String redaction) {
        if (nodeId == null || !NODE_ID.matcher(nodeId).matches()) {
            return Mono.just(ResponseEntity.badRequest().body("Missing or invalid X-JobRunr-Node header"));
        }
        return authenticate(authorization).flatMap(owner -> {
            if (owner.isEmpty()) return Mono.just(unauthorized());
            Account account = store.findById(owner.get().accountId()).orElse(null);
            if (account == null) return Mono.just(unauthorized());
            ConnectorHub.Registration registration;
            try {
                registration = hub.register(account.id(), nodeId, new ConnectorHub.NodeInfo(
                        limit(jobrunrVersion), limit(javaVersion), limit(framework), Boolean.parseBoolean(redaction),
                        "operate".equals(owner.get().scope())), account.proEnabled());
            } catch (ConnectorHub.NodeLimitException | ConnectorHub.NodeRegistrationRateException |
                     ConnectorHub.GlobalNodeLimitException e) {
                return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                        .contentType(MediaType.TEXT_PLAIN).body(e.getMessage()));
            }
            ConnectorHub.Node node = registration.node();
            if (!account.proEnabled() && node.isReady() && account.clusterId() != null &&
                    !account.clusterId().equals(node.clusterId())) {
                node.reject(InstanceMonitor.SECOND_CLUSTER_MESSAGE);
            }
            if (node.rejectedReason() != null) {
                hub.forget(node);
                return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).contentType(MediaType.TEXT_PLAIN).body(node.rejectedReason()));
            }
            if (registration.isNew()) monitor.onNodeConnected(node);

            return hub.awaitRequests(node).map(lines -> lines.isEmpty()
                    ? ResponseEntity.noContent().<String>build()
                    : ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(String.join("\n", lines) + "\n"))
                    .onErrorResume(ConnectorHub.AlreadyPollingException.class, e -> Mono.just(ResponseEntity
                            .status(HttpStatus.CONFLICT).contentType(MediaType.TEXT_PLAIN).body(e.getMessage())));
        }).onErrorResume(AuthBusyException.class, e -> Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1").contentType(MediaType.TEXT_PLAIN).body("Connector authentication is busy; retry shortly.")));
    }

    @PostMapping("/connector/results/{requestId}")
    public Mono<ResponseEntity<Void>> result(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestHeader(value = "X-JobRunr-Node", required = false) String nodeId,
            @RequestHeader(value = "X-JobRunr-Status", defaultValue = "200") int status,
            @PathVariable String requestId,
            ServerWebExchange exchange) {
        if (nodeId == null || !NODE_ID.matcher(nodeId).matches() ||
                requestId == null || !REQUEST_ID.matcher(requestId).matches() ||
                status < 200 || status > 599) {
            return Mono.just(ResponseEntity.badRequest().build());
        }
        return authenticate(authorization).flatMap(owner -> {
            if (owner.isEmpty()) return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).<Void>build());
            return DataBufferUtils.join(exchange.getRequest().getBody(), MAX_RESULT_BYTES)
                    .map(buffer -> {
                        try {
                            byte[] bytes = new byte[buffer.readableByteCount()];
                            buffer.read(bytes);
                            return complete(owner.get().accountId(), nodeId, requestId, status,
                                    new String(bytes, StandardCharsets.UTF_8));
                        } finally {
                            DataBufferUtils.release(buffer);
                        }
                    })
                    .switchIfEmpty(Mono.fromSupplier(() -> complete(owner.get().accountId(), nodeId, requestId, status, null)));
        }).onErrorResume(DataBufferLimitException.class,
                e -> Mono.just(ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).<Void>build()))
                .onErrorResume(AuthBusyException.class, e -> Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                        .header("Retry-After", "1").<Void>build()));
    }

    private ResponseEntity<Void> complete(String accountId, String nodeId, String requestId, int status, String body) {
        return hub.complete(accountId, nodeId, requestId, status, body)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    private Mono<Optional<AccountStore.TokenOwner>> authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) return Mono.just(Optional.empty());
        String token = authorization.substring("Bearer ".length()).trim();
        if (!CONNECTOR_TOKEN.matcher(token).matches()) return Mono.just(Optional.empty());
        return Mono.defer(() -> {
            if (!authLookups.tryAcquire()) return Mono.error(new AuthBusyException());
            return Mono.fromCallable(() -> store.findActiveToken(token, AccountStore.KIND_CONNECTOR))
                    .subscribeOn(Schedulers.boundedElastic())
                    .doFinally(signal -> authLookups.release());
        });
    }

    private static final class AuthBusyException extends RuntimeException {}

    private static ResponseEntity<String> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).contentType(MediaType.TEXT_PLAIN)
                .body("Unknown or revoked connector token. Get a new one at https://mcp.jobrunr.io/account");
    }

    private static String limit(String value) {
        if (value == null) return null;
        return value.length() <= 40 ? value : value.substring(0, 40);
    }
}
