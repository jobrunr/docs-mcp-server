package io.jobrunr.docsmcp.admin;

import io.jobrunr.docsmcp.index.DocsLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@RequestMapping("/admin")
public class ReindexController {

    private static final Logger log = LoggerFactory.getLogger(ReindexController.class);
    private static final long CLOCK_SKEW_SECONDS = 300;
    private static final int MAX_USED_SIGNATURES = 10_000;

    private final DocsLoader loader;
    private final String secret;
    private final Map<String, Long> usedSignatures = new ConcurrentHashMap<>();
    private final AtomicBoolean reindexRunning = new AtomicBoolean();

    public ReindexController(DocsLoader loader, @Value("${docs.reindex-secret:}") String secret) {
        this.loader = loader;
        this.secret = secret == null ? "" : secret;
    }

    @PostMapping("/reindex")
    public ResponseEntity<String> reindex(
            @RequestHeader(value = "X-Reindex-Timestamp", required = false) String timestamp,
            @RequestHeader(value = "X-Reindex-Signature", required = false) String signature) {
        if (secret.isBlank()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body("reindex secret not configured");
        }
        if (timestamp == null || signature == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("missing signature headers");
        }
        long ts;
        try {
            ts = Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("bad timestamp");
        }
        long now = System.currentTimeMillis() / 1000;
        if (ts < now - CLOCK_SKEW_SECONDS || ts > now + CLOCK_SKEW_SECONDS) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("timestamp out of window");
        }
        String expected = hmacHex(timestamp, secret);
        if (!constantTimeEquals(expected, signature)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("bad signature");
        }
        // The signed timestamp is a one-use event key for its entire validity window.
        // This preserves existing webhook signing while making captured requests non-replayable.
        synchronized (usedSignatures) {
            usedSignatures.entrySet().removeIf(entry -> entry.getValue() < now);
            if (usedSignatures.containsKey(signature)) {
                return ResponseEntity.status(HttpStatus.CONFLICT).body("signature already used");
            }
            if (usedSignatures.size() >= MAX_USED_SIGNATURES) {
                return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("too many reindex requests");
            }
            usedSignatures.put(signature, ts + CLOCK_SKEW_SECONDS);
        }
        if (!reindexRunning.compareAndSet(false, true)) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("reindex already running");
        }
        Mono.fromRunnable(() -> {
                    try {
                        loader.reload(true);
                    } catch (Exception e) {
                        log.warn("Manual reindex failed: {}", e.toString());
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signal -> reindexRunning.set(false))
                .subscribe();
        return ResponseEntity.accepted().body("reindex started");
    }

    private static String hmacHex(String message, String key) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] digest = mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) return false;
        byte[] ab = a.getBytes(StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(ab, bb);
    }
}
