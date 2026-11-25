package io.jobrunr.docsmcp.trial;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TrialToolsTest {

    private RecordingClient client;
    private TrialTools tools;

    @BeforeEach
    void setup() {
        client = new RecordingClient();
        tools = new TrialTools(client);
    }

    @Test
    void submitsValidRequest() {
        TrialResponse resp = tools.requestJobrunrProTrial(
                "alice@acme.com", "Acme Corp", "priority-queues", "Background billing reconciliation");
        assertThat(resp.success()).isTrue();
        assertThat(resp.message()).contains("alice@acme.com");
        assertThat(client.calls).hasSize(1);
        RecordingClient.Call call = client.calls.get(0);
        assertThat(call.email).isEqualTo("alice@acme.com");
        assertThat(call.company).isEqualTo("Acme Corp");
        assertThat(call.interestedIn).isEqualTo("priority-queues");
        assertThat(call.useCase).isEqualTo("Background billing reconciliation");
    }

    @Test
    void omitsOptionalFieldsWhenAbsent() {
        TrialResponse resp = tools.requestJobrunrProTrial(
                "bob@example.io", "Example Co", null, null);
        assertThat(resp.success()).isTrue();
        assertThat(client.calls).hasSize(1);
        RecordingClient.Call call = client.calls.get(0);
        assertThat(call.interestedIn).isNull();
        assertThat(call.useCase).isNull();
    }

    @Test
    void rejectsInvalidEmail() {
        TrialResponse resp = tools.requestJobrunrProTrial(
                "not-an-email", "Acme Corp", null, null);
        assertThat(resp.success()).isFalse();
        assertThat(resp.message()).containsIgnoringCase("email");
        assertThat(client.calls).isEmpty();
    }

    @Test
    void rejectsBlankEmail() {
        TrialResponse resp = tools.requestJobrunrProTrial("   ", "Acme Corp", null, null);
        assertThat(resp.success()).isFalse();
        assertThat(client.calls).isEmpty();
    }

    @Test
    void rejectsOversizedEmailBeforeRetainingIt() {
        TrialResponse resp = tools.requestJobrunrProTrial("a".repeat(321) + "@example.com", "Acme Corp", null, null);
        assertThat(resp.success()).isFalse();
        assertThat(client.calls).isEmpty();
    }

    @Test
    void rejectsBlankCompany() {
        TrialResponse resp = tools.requestJobrunrProTrial("alice@acme.com", "  ", null, null);
        assertThat(resp.success()).isFalse();
        assertThat(resp.message()).containsIgnoringCase("company");
        assertThat(client.calls).isEmpty();
    }

    @Test
    void surfacesWebhookFailureWithFallback() {
        client.nextResult = new TrialClient.Result(false, "Webhook returned 500");
        TrialResponse resp = tools.requestJobrunrProTrial(
                "alice@acme.com", "Acme Corp", null, null);
        assertThat(resp.success()).isFalse();
        assertThat(resp.message()).contains("sales@jobrunr.io");
    }

    @Test
    void truncatesOverlongOptionalFields() {
        String longInterest = "x".repeat(500);
        String longUseCase = "y".repeat(2000);
        TrialResponse resp = tools.requestJobrunrProTrial(
                "alice@acme.com", "Acme Corp", longInterest, longUseCase);
        assertThat(resp.success()).isTrue();
        RecordingClient.Call call = client.calls.get(0);
        assertThat(call.interestedIn).hasSize(200);
        assertThat(call.useCase).hasSize(500);
    }

    @Test
    void trimsWhitespace() {
        tools.requestJobrunrProTrial("  alice@acme.com  ", "  Acme Corp  ", "  priority  ", "  use case  ");
        RecordingClient.Call call = client.calls.get(0);
        assertThat(call.email).isEqualTo("alice@acme.com");
        assertThat(call.company).isEqualTo("Acme Corp");
        assertThat(call.interestedIn).isEqualTo("priority");
        assertThat(call.useCase).isEqualTo("use case");
    }

    @Test
    void repeatedEmailIsSubmittedOnlyOnceWithinCooldownEvenWithDifferentCase() {
        TrialResponse first = tools.requestJobrunrProTrial("Alice@Acme.com", "Acme", null, null);
        TrialResponse repeated = tools.requestJobrunrProTrial(" alice@acme.com ", "Other Company", null, null);

        assertThat(first.success()).isTrue();
        assertThat(repeated.success()).isTrue();
        assertThat(client.calls).hasSize(1);
    }

    @Test
    void webhookFailureIsThrottledAndCanBeRetriedAfterFiveMinutes() {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-25T12:00:00Z"));
        tools = new TrialTools(client, now::get, 2);
        client.nextResult = new TrialClient.Result(false, "Webhook returned 500");
        assertThat(tools.requestJobrunrProTrial("alice@acme.com", "Acme", null, null).success()).isFalse();
        assertThat(tools.requestJobrunrProTrial("alice@acme.com", "Acme", null, null).success()).isFalse();
        assertThat(client.calls).hasSize(1);

        now.set(now.get().plus(Duration.ofMinutes(5)).plusSeconds(1));
        client.nextResult = new TrialClient.Result(true, "Submitted");
        assertThat(tools.requestJobrunrProTrial("alice@acme.com", "Acme", null, null).success()).isTrue();
        assertThat(client.calls).hasSize(2);
    }

    @Test
    void recentEmailCacheIsBoundedAndAnExpiredEntryFreesSpace() {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-11-25T12:00:00Z"));
        tools = new TrialTools(client, now::get, 2);
        assertThat(tools.requestJobrunrProTrial("a@example.com", "A", null, null).success()).isTrue();
        assertThat(tools.requestJobrunrProTrial("b@example.com", "B", null, null).success()).isTrue();
        assertThat(tools.requestJobrunrProTrial("c@example.com", "C", null, null).success()).isFalse();
        assertThat(client.calls).hasSize(2);

        now.set(now.get().plus(Duration.ofDays(1)).plusSeconds(1));
        assertThat(tools.requestJobrunrProTrial("c@example.com", "C", null, null).success()).isTrue();
        assertThat(client.calls).hasSize(3);
    }

    @Test
    void concurrentRequestsForOneAddressSubmitOnlyOnce() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TrialClient slowClient = new TrialClient("http://localhost/unused", Duration.ofSeconds(1)) {
            @Override
            public Result submit(String email, String company, String interestedIn, String useCase) {
                calls.incrementAndGet();
                entered.countDown();
                try {
                    if (!finish.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out waiting for test");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return new Result(true, "Submitted");
            }
        };
        TrialTools concurrentTools = new TrialTools(slowClient);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> concurrentTools.requestJobrunrProTrial("alice@acme.com", "Acme", null, null));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                TrialResponse inFlight = concurrentTools.requestJobrunrProTrial("ALICE@ACME.COM", "Acme", null, null);
                assertThat(inFlight.success()).isFalse();
                assertThat(inFlight.message()).contains("already being processed");
            } finally {
                finish.countDown();
            }
            assertThat(first.get(5, TimeUnit.SECONDS).success()).isTrue();
            assertThat(concurrentTools.requestJobrunrProTrial("alice@acme.com", "Acme", null, null).success()).isTrue();
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    /** Test double that records submit() args and returns a configurable result. */
    static final class RecordingClient extends TrialClient {
        record Call(String email, String company, String interestedIn, String useCase) {}
        final List<Call> calls = new ArrayList<>();
        Result nextResult = new Result(true, "Submitted");

        RecordingClient() {
            super("http://localhost/unused", Duration.ofSeconds(1));
        }

        @Override
        public Result submit(String email, String company, String interestedIn, String useCase) {
            calls.add(new Call(email, company, interestedIn, useCase));
            return nextResult;
        }
    }
}
