package io.jobrunr.docsmcp.hub.account;

import io.jobrunr.docsmcp.hub.HubProperties;
import io.jobrunr.docsmcp.hub.connector.ConnectorHub;
import io.jobrunr.docsmcp.hub.connector.InstanceMonitor;
import io.jobrunr.docsmcp.hub.leads.LeadEvents;
import io.jobrunr.docsmcp.web.ClientIpExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * Signup, passwordless sign-in and the account page API. Pages live in {@code static/}; they call these endpoints.
 */
@RestController
public class AccountController {

    static final String SESSION_COOKIE = "jr_mcp_session";

    private static final Logger log = LoggerFactory.getLogger(AccountController.class);

    private static final Pattern EMAIL = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final Duration EMAIL_COOLDOWN = Duration.ofSeconds(60);
    private static final int MAX_LINKS_PER_IP_PER_HOUR = 10;
    private static final int MAX_RATE_LIMIT_KEYS = 10_000;

    private final AccountStore store;
    private final Mailer mailer;
    private final LeadEvents leadEvents;
    private final ConnectorHub hub;
    private final InstanceMonitor monitor;
    private final HubProperties properties;
    private final Map<String, Instant> lastLinkPerEmail = boundedMap(MAX_RATE_LIMIT_KEYS);
    private final Map<String, List<Instant>> linksPerIp = boundedMap(MAX_RATE_LIMIT_KEYS);

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
            if (!mailer.isConfigured()) return mailNotConfigured();
            if (!allowLink(email, exchange)) return tooManyRequests();
            Optional<Account> existing = store.findByEmail(email);
            Account account = existing.orElseGet(() -> store.create(email, name, company, trim(request.role(), 200), trim(request.useCase(), 1000)));
            return sendLink(account, account.verifiedAt() == null);
        });
    }

    @PostMapping("/api/signin")
    public Mono<ResponseEntity<Map<String, Object>>> signIn(@RequestBody SignInRequest request, ServerWebExchange exchange) {
        String email = trim(request.email(), 320);
        if (email == null || !EMAIL.matcher(email).matches()) return badRequest("Please enter a valid email address.");
        return blocking(() -> {
            if (!mailer.isConfigured()) return mailNotConfigured();
            if (!allowLink(email, exchange)) return tooManyRequests();
            // same answer whether or not the account exists, so the form cannot be used to probe for accounts
            return store.findByEmail(email)
                    .map(account -> sendLink(account, account.verifiedAt() == null))
                    .orElseGet(() -> {
                        // Keep the HTTP answer indistinguishable, but leave a domain-only clue for operators
                        // when a deployment lost its account database.
                        log.debug("Sign-in link not sent: no account found at domain {}. Check HUB_DB_URL and /app/data persistence if this is a known user.",
                                email.substring(email.lastIndexOf('@') + 1));
                        return ResponseEntity.ok(Map.of("status", "check-your-inbox"));
                    });
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
                leadEvents.record(account.get(), LeadEvents.VERIFIED, account.get().useCase(), ClientIpExtractor.from(exchange.getRequest()));
            }
            String session = store.createSession(account.get().id(), properties.sessionTtl());
            // This user proved mailbox access. A just-verified signup can sign in again
            // immediately after signing out without hitting the preceding signup cooldown.
            releaseLinkCooldown(account.get().email());
            exchange.getResponse().addCookie(ResponseCookie.from(SESSION_COOKIE, session)
                    .httpOnly(true)
                    .secure(secureCookie())
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
            exchange.getResponse().addCookie(ResponseCookie.from(SESSION_COOKIE, "")
                    .httpOnly(true).secure(secureCookie()).sameSite("Lax").path("/").maxAge(0).build());
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
            body.put("proEnabled", account.proEnabled());
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

    private ResponseEntity<Map<String, Object>> sendLink(Account account, boolean firstTime) {
        String secret = store.createMagicLink(account.id(), properties.magicLinkTtl());
        try {
            mailer.sendSignInLink(account, properties.publicUrl() + "/verify#" + secret, firstTime);
        } catch (RuntimeException e) {
            log.warn("Could not send the sign-in link to {}: {}", account.emailDomain(), e.toString());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(Map.of("error", "We could not send the email. Please try again in a few minutes."));
        }
        return ResponseEntity.ok(Map.of("status", "check-your-inbox"));
    }

    private static ResponseEntity<Map<String, Object>> mailNotConfigured() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "This server cannot send email yet, so signups are paused. Please try again later."));
    }

    private synchronized boolean allowLink(String email, ServerWebExchange exchange) {
        Instant now = Instant.now();
        String normalizedEmail = email.toLowerCase(Locale.ROOT);
        Instant last = lastLinkPerEmail.get(normalizedEmail);
        if (last != null && last.isAfter(now.minus(EMAIL_COOLDOWN))) return false;

        String ip = String.valueOf(ClientIpExtractor.from(exchange.getRequest()));
        List<Instant> previous = linksPerIp.get(ip);
        List<Instant> recent = previous == null ? new java.util.ArrayList<>()
                : new java.util.ArrayList<>(previous.stream().filter(t -> t.isAfter(now.minus(Duration.ofHours(1)))).toList());
        if (recent.size() >= MAX_LINKS_PER_IP_PER_HOUR) {
            linksPerIp.put(ip, recent);
            return false;
        }
        recent.add(now);
        linksPerIp.put(ip, recent);
        lastLinkPerEmail.put(normalizedEmail, now);
        return true;
    }

    private synchronized void releaseLinkCooldown(String email) {
        lastLinkPerEmail.remove(email.toLowerCase(Locale.ROOT));
    }

    private boolean secureCookie() {
        return "https".equalsIgnoreCase(URI.create(properties.publicUrl()).getScheme());
    }

    private static <V> Map<String, V> boundedMap(int maximumSize) {
        return new LinkedHashMap<>(128, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
                return size() > maximumSize;
            }
        };
    }

    private static <T> Mono<T> blocking(Callable<T> callable) {
        return Mono.fromCallable(callable).subscribeOn(Schedulers.boundedElastic());
    }

    private static Mono<ResponseEntity<Map<String, Object>>> badRequest(String message) {
        return Mono.just(ResponseEntity.badRequest().body(Map.of("error", message)));
    }

    private static ResponseEntity<Map<String, Object>> tooManyRequests() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of("error", "Too many link requests. Check your inbox, or try again later."));
    }

    private static String trim(String value, int max) {
        if (value == null || value.isBlank()) return null;
        String trimmed = value.trim();
        return trimmed.length() <= max ? trimmed : trimmed.substring(0, max);
    }
}
