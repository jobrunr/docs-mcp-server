package io.jobrunr.docsmcp.admin;

import io.jobrunr.docsmcp.index.DocsLoader;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class ReindexControllerTest {

    private static final String SECRET = "local-test-only-secret";

    @Test
    void aSignedRequestIsAcceptedOnceAndExactReplayIsRejected() throws Exception {
        DocsLoader loader = mock(DocsLoader.class);
        ReindexController controller = new ReindexController(loader, SECRET);
        String timestamp = Long.toString(Instant.now().getEpochSecond());
        String signature = sign(timestamp);

        assertThat(controller.reindex(timestamp, signature).getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(controller.reindex(timestamp, signature).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        verify(loader, timeout(2000).times(1)).reload(true);
    }

    @Test
    void distinctSignedRequestsCannotRunConcurrentReloads() throws Exception {
        DocsLoader loader = mock(DocsLoader.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            started.countDown();
            release.await(3, TimeUnit.SECONDS);
            return null;
        }).when(loader).reload(true);
        ReindexController controller = new ReindexController(loader, SECRET);
        long now = Instant.now().getEpochSecond();
        String first = Long.toString(now);
        String second = Long.toString(now + 1);

        try {
            assertThat(controller.reindex(first, sign(first)).getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(controller.reindex(second, sign(second)).getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        } finally {
            release.countDown();
        }
        verify(loader, timeout(2000).times(1)).reload(eq(true));
    }

    private static String sign(String timestamp) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(timestamp.getBytes(StandardCharsets.UTF_8)));
    }
}
