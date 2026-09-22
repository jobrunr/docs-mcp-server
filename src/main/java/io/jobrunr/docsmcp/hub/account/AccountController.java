package io.jobrunr.docsmcp.hub.account;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.connector.ConnectorHub;
import io.jobrunr.docsmcp.hub.connector.InstanceMonitor;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import io.jobrunr.docsmcp.web.ClientIpExtractor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Signup, passwordless sign-in and the account page API. Pages live in {@code static/}; they call these endpoints.
 */
@RestController
public class AccountController {

    static final String SESSION_COOKIE = "jr_mcp_session";

    private static final Pattern EMAIL = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final Duration EMAIL_COOLDOWN = Duration.ofSeconds(60);
    private static final int MAX_LINKS_PER_IP_PER_HOUR = 10;

    private final AccountStore store;
    private final Mailer mailer;
    private final LeadEvents leadEvents;
    private final ConnectorHub hub;
    private final InstanceMonitor monitor;
    private final HubProperties properties;
    private final Map<String, Instant> lastLinkPerEmail = new ConcurrentHashMap<>();
    private final Map<String, List<Instant>> linksPerIp = new ConcurrentHashMap<>();

    public AccountController(AccountStore store, Mailer mailer, LeadEvents leadEvents, ConnectorHub hub, InstanceMonitor monitor, HubProperties properties) {
        this.store = store;
        this.mailer = mailer;
        this.leadEvents = leadEvents;
        this.hub = hub;
        this.monitor = monitor;
        this.properties = properties;
    }

    public record SignupRequest(String name, String email, String company, String role, String useCase) {
    }

    public record SignInRequest(String email) {
    }

    public record VerifyRequest(String token) {
    }

    public record TokensRequest(boolean operate) {
    }

    @PostMapping("/api/signup")
    public Mono<ResponseEntity<Map<String, Object>>> signup(@RequestBody SignupRequest request, ServerWebExchange exchange) {
        String email = trim(request.email(), 320);
        String name = trim(request.name(), 200);
        String company = trim(request.company(), 200);
        if (email == null || !EMAIL.matcher(email).matches()) return badRequest("Please enter a valid email address.");
        if (name == null) return badRequest("Please enter your name.");
        if (company == null) return badRequest("Please enter your company.");

        return blocking(() -> {
            if (!allowLink(email, exchange)) return tooManyRequests();
            Optional<Account> existing = store.findByEmail(email);
            Account account = existing.orElseGet(() -> store.create(email, name, company, trim(request.role(), 200), trim(request.useCase(), 1000)));
            sendLink(account, account.verifiedAt() == null);
            return ResponseEntity.ok(Map.<String, Object>of("status", "check-your-inbox"));
        });
    }

    @PostMapping("/api/signin")
    public Mono<ResponseEntity<Map<String, Object>>> signIn(@RequestBody SignInRequest request, ServerWebExchange exchange) {
        String email = trim(request.email(), 320);
        if (email == null || !EMAIL.matcher(email).matches()) return badRequest("Please enter a valid email address.");
        return blocking(() -> {
            if (!allowLink(email, exchange)) return tooManyRequests();
            // same answer whether or not the account exists, so the form cannot be used to probe for accounts
            store.findByEmail(email).ifPresent(account -> sendLink(account, account.verifiedAt() == null));
            return ResponseEntity.ok(Map.<String, Object>of("status", "check-your-inbox"));
        });
    }

    @PostMapping("/api/verify")
    public Mono<ResponseEntity<Map<String, Object>>> verify(@RequestBody VerifyRequest request, ServerWebExchange exchange) {
        if (request.token() == null || request.token().isBlank()) return badRequest("This sign-in link is not valid.");
        return blocking(() -> {
            Optional<Account> account = store.consumeMagicLink(request.token().trim()).flatMap(store::findById);
            if (account.isEmpty()) {
                return ResponseEntity.status(HttpStatus.GONE).body(Map.<String, Object>of("error", "This sign-in link has expired or was already used. Request a new one."));
            }
            if (store.markVerified(account.get().id())) {
                leadEvents.record(account.get(), LeadEvents.VERIFIED, account.get().useCase());
            }
            String session = store.createSession(account.get().id(), properties.sessionTtl());
            exchange.getResponse().addCookie(ResponseCookie.from(SESSION_COOKIE, session)
                    .httpOnly(true)
                    .secure(isHttps(exchange))
                    .sameSite("Lax")
                    .path("/")
                    .maxAge(properties.sessionTtl())
                    .build());
            return ResponseEntity.ok(Map.<String, Object>of("status", "signed-in"));
        });
    }

    @PostMapping("/api/signout")
    public Mono<ResponseEntity<Void>> signOut(ServerWebExchange exchange) {
        var cookie = exchange.getRequest().getCookies().getFirst(SESSION_COOKIE);
        return blocking(() -> {
            if (cookie != null) store.deleteSession(cookie.getValue());
            exchange.getResponse().addCookie(ResponseCookie.from(SESSION_COOKIE, "").path("/").maxAge(0).build());
            return ResponseEntity.noContent().build();
        });
    }

    @GetMapping("/api/account")
    public Mono<ResponseEntity<Map<String, Object>>> account(ServerWebExchange exchange) {
        return withAccount(exchange, account -> {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("name", account.name());
            body.put("email", account.email());
            body.put("company", account.company());
            body.put("publicUrl", properties.publicUrl());
            body.put("tokens", store.activeTokens(account.id()));
            body.put("nodes", hub.onlineNodes(account.id()).stream().map(node -> {
                Map<String, Object> n = new LinkedHashMap<>();
                n.put("jobrunrVersion", node.info().jobrunrVersion());
                n.put("javaVersion", node.info().javaVersion());
                n.put("framework", node.info().framework());
                n.put("redaction", node.info().redaction());
                n.put("connectedAt", node.connectedAt());
                n.put("lastSeenAt", node.lastPollAt());
                return n;
            }).toList());
            body.put("instance", store.findInstance(account.id()).orElse(null));
            monitor.refreshSoon(account.id());
            body.put("recentToolCalls", store.recentToolCalls(account.id(), 15));
            return ResponseEntity.ok(body);
        });
    }

    @PostMapping("/api/account/tokens")
    public Mono<ResponseEntity<Map<String, Object>>> issueTokens(@RequestBody TokensRequest request, ServerWebExchange exchange) {
        return withAccount(exchange, account -> {
            AccountStore.IssuedTokens tokens = store.issueTokens(account.id(), request.operate());
            hub.disconnectAccount(account.id());
            return ResponseEntity.ok(Map.<String, Object>of(
                    "connectorToken", tokens.connectorToken(),
                    "agentToken", tokens.agentToken(),
                    "scope", request.operate() ? "operate" : "read"));
        });
    }

    private Mono<ResponseEntity<Map<String, Object>>> withAccount(ServerWebExchange exchange, java.util.function.Function<Account, ResponseEntity<Map<String, Object>>> action) {
        var cookie = exchange.getRequest().getCookies().getFirst(SESSION_COOKIE);
        if (cookie == null) return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Not signed in")));
        return blocking(() -> store.accountForSession(cookie.getValue())
                .flatMap(store::findById)
                .map(action)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Not signed in"))));
    }

    private void sendLink(Account account, boolean firstTime) {
        String secret = store.createMagicLink(account.id(), properties.magicLinkTtl());
        mailer.sendSignInLink(account, properties.publicUrl() + "/verify#" + secret, firstTime);
    }

    private boolean allowLink(String email, ServerWebExchange exchange) {
        Instant now = Instant.now();
        Instant last = lastLinkPerEmail.get(email.toLowerCase());
        if (last != null && last.isAfter(now.minus(EMAIL_COOLDOWN))) return false;

        String ip = String.valueOf(ClientIpExtractor.from(exchange.getRequest()));
        List<Instant> recent = linksPerIp.compute(ip, (key, list) -> {
            List<Instant> kept = list == null ? new java.util.ArrayList<>() : new java.util.ArrayList<>(list.stream().filter(t -> t.isAfter(now.minus(Duration.ofHours(1)))).toList());
            kept.add(now);
            return kept;
        });
        if (recent.size() > MAX_LINKS_PER_IP_PER_HOUR) return false;
        lastLinkPerEmail.put(email.toLowerCase(), now);
        return true;
    }

    private static boolean isHttps(ServerWebExchange exchange) {
        String forwardedProto = exchange.getRequest().getHeaders().getFirst("X-Forwarded-Proto");
        return "https".equalsIgnoreCase(forwardedProto) || "https".equalsIgnoreCase(exchange.getRequest().getURI().getScheme());
    }

    private static <T> Mono<T> blocking(Callable<T> callable) {
        return Mono.fromCallable(callable).subscribeOn(Schedulers.boundedElastic());
    }

    private static Mono<ResponseEntity<Map<String, Object>>> badRequest(String message) {
        return Mono.just(ResponseEntity.badRequest().body(Map.of("error", message)));
    }

    private static ResponseEntity<Map<String, Object>> tooManyRequests() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("error", "We just sent you a link. Check your inbox, or try again in a minute."));
    }

    private static String trim(String value, int max) {
        if (value == null || value.isBlank()) return null;
        String trimmed = value.trim();
        return trimmed.length() <= max ? trimmed : trimmed.substring(0, max);
    }
}
