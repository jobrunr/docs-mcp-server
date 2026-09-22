package io.jobrunr.docsmcp.hub.connector;

import io.jobrunr.docsmcp.hub.account.AccountStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The two endpoints the connector inside a customer's JVM talks to. Both authenticate with the connector token.
 */
@RestController
public class ConnectorController {

    private static final Pattern NODE_ID = Pattern.compile("[A-Za-z0-9-]{8,64}");

    private final ConnectorHub hub;
    private final InstanceMonitor monitor;
    private final AccountStore store;

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

            ConnectorHub.Registration registration = hub.register(owner.get().accountId(), nodeId, new ConnectorHub.NodeInfo(
                    limit(jobrunrVersion), limit(javaVersion), limit(framework), !"false".equals(redaction)));
            ConnectorHub.Node node = registration.node();
            if (node.rejectedReason() != null) {
                hub.forget(node);
                return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).contentType(MediaType.TEXT_PLAIN).body(node.rejectedReason()));
            }
            if (registration.isNew()) monitor.onNodeConnected(node);

            return hub.awaitRequests(node).map(lines -> lines.isEmpty()
                    ? ResponseEntity.noContent().<String>build()
                    : ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(String.join("\n", lines) + "\n"));
        });
    }

    @PostMapping("/connector/results/{requestId}")
    public Mono<ResponseEntity<Void>> result(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestHeader(value = "X-JobRunr-Status", defaultValue = "200") int status,
            @PathVariable String requestId,
            @RequestBody(required = false) String body) {
        return authenticate(authorization).map(owner -> {
            if (owner.isEmpty()) return ResponseEntity.status(HttpStatus.UNAUTHORIZED).<Void>build();
            return hub.complete(owner.get().accountId(), requestId, status, body)
                    ? ResponseEntity.noContent().<Void>build()
                    : ResponseEntity.notFound().<Void>build();
        });
    }

    private Mono<Optional<AccountStore.TokenOwner>> authenticate(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer jrc_")) return Mono.just(Optional.empty());
        String token = authorization.substring("Bearer ".length()).trim();
        return Mono.fromCallable(() -> store.findActiveToken(token, AccountStore.KIND_CONNECTOR)).subscribeOn(Schedulers.boundedElastic());
    }

    private static ResponseEntity<String> unauthorized() {
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).contentType(MediaType.TEXT_PLAIN)
                .body("Unknown or revoked connector token. Get a new one at https://mcp.jobrunr.io/account");
    }

    private static String limit(String value) {
        if (value == null) return null;
        return value.length() <= 40 ? value : value.substring(0, 40);
    }
}
