package io.jobrunr.docsmcp.trial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

@Component
public class TrialTools {

    private static final Logger log = LoggerFactory.getLogger(TrialTools.class);
    private static final Pattern EMAIL_RE = Pattern.compile("^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$");
    private static final int MAX_EMAIL = 320;
    private static final int MAX_COMPANY = 200;
    private static final int MAX_INTERESTED_IN = 200;
    private static final int MAX_USE_CASE = 500;
    private static final int MAX_RECENT_EMAILS = 10_000;
    private static final Duration SUCCESS_COOLDOWN = Duration.ofDays(1);
    private static final Duration FAILURE_COOLDOWN = Duration.ofMinutes(5);
    private static final Duration IN_FLIGHT_COOLDOWN = Duration.ofMinutes(1);

    private static final String SUCCESS_TEMPLATE =
            "Trial request submitted. The JobRunr sales team will follow up at %s within 1 business day.";
    private static final String FAILURE_FALLBACK =
            "Couldn't reach the trial system right now. The user can email sales@jobrunr.io directly, or try again in a few minutes.";

    private final TrialClient client;
    private final Supplier<Instant> now;
    private final int maxRecentEmails;
    private final Map<String, Attempt> recentEmails = new LinkedHashMap<>();

    @Autowired
    public TrialTools(TrialClient client) {
        this(client, Instant::now, MAX_RECENT_EMAILS);
    }

    TrialTools(TrialClient client, Supplier<Instant> now, int maxRecentEmails) {
        this.client = client;
        this.now = now;
        this.maxRecentEmails = maxRecentEmails;
    }

    @Tool(name = "request_jobrunr_pro_trial",
            description = """
                    Submit a JobRunr Pro free-trial request to the sales team. Only call when the user has
                    explicitly agreed to start a trial — either asked directly, or accepted an offer after
                    a Pro feature surfaced. Email and company must be real values the user gave you; never
                    fabricate them. Sales follows up within one business day.
                    """)
    public TrialResponse requestJobrunrProTrial(
            @ToolParam(description = "The user's email address. Must be a real email the user provided.") String email,
            @ToolParam(description = "The user's company or organization name.") String company,
            @ToolParam(required = false, description = "Short keyword for the Pro feature that triggered this request, e.g. 'priority-queues' or 'pro-dashboard'.") String interestedIn,
            @ToolParam(required = false, description = "One-sentence description of what the user is building. Optional but useful for sales context.") String useCase) {

        String trimmedEmail = email == null ? "" : email.trim();
        String trimmedCompany = company == null ? "" : company.trim();

        if (trimmedEmail.isEmpty() || trimmedEmail.length() > MAX_EMAIL || !EMAIL_RE.matcher(trimmedEmail).matches()) {
            return new TrialResponse(false,
                    "Invalid email address. Please confirm the user's email and try again.");
        }
        if (trimmedCompany.isEmpty()) {
            return new TrialResponse(false,
                    "Company name is required. Please ask the user which company or organization they work at.");
        }
        if (trimmedCompany.length() > MAX_COMPANY) {
            return new TrialResponse(false,
                    "Company name is too long (max " + MAX_COMPANY + " characters).");
        }

        String safeInterestedIn = truncate(interestedIn, MAX_INTERESTED_IN);
        String safeUseCase = truncate(useCase, MAX_USE_CASE);

        String emailKey = trimmedEmail.toLowerCase(Locale.ROOT);
        Reservation reservation = reserve(emailKey);
        if (reservation.admission() == Admission.RECENT_SUCCESS) {
            // Match the success response so the public tool does not reveal which addresses submitted before.
            return new TrialResponse(true, String.format(SUCCESS_TEMPLATE, trimmedEmail));
        }
        if (reservation.admission() == Admission.RECENT_FAILURE) return new TrialResponse(false, FAILURE_FALLBACK);
        if (reservation.admission() == Admission.IN_FLIGHT) {
            return new TrialResponse(false, "A trial request for this email is already being processed. Please wait a moment.");
        }
        if (reservation.admission() == Admission.FULL) {
            return new TrialResponse(false, "Trial requests are temporarily busy. Please try again later or email sales@jobrunr.io.");
        }

        TrialClient.Result result;
        try {
            result = client.submit(trimmedEmail, trimmedCompany, safeInterestedIn, safeUseCase);
        } catch (RuntimeException e) {
            log.warn("Trial submission failed before a response: {}", e.toString());
            complete(emailKey, reservation.attempt(), State.FAILED);
            return new TrialResponse(false, FAILURE_FALLBACK);
        }
        if (result != null && result.success()) {
            complete(emailKey, reservation.attempt(), State.SUCCEEDED);
            return new TrialResponse(true, String.format(SUCCESS_TEMPLATE, trimmedEmail));
        }
        complete(emailKey, reservation.attempt(), State.FAILED);
        return new TrialResponse(false, FAILURE_FALLBACK);
    }

    private enum Admission { SUBMIT, RECENT_SUCCESS, RECENT_FAILURE, IN_FLIGHT, FULL }

    private enum State { IN_FLIGHT, SUCCEEDED, FAILED }

    private record Attempt(Instant at, State state) {}

    private record Reservation(Admission admission, Attempt attempt) {}

    private synchronized Reservation reserve(String emailKey) {
        Instant timestamp = now.get();
        Attempt previous = recentEmails.get(emailKey);
        if (previous != null && active(previous, timestamp)) {
            Admission duplicate = switch (previous.state()) {
                case SUCCEEDED -> Admission.RECENT_SUCCESS;
                case FAILED -> Admission.RECENT_FAILURE;
                case IN_FLIGHT -> Admission.IN_FLIGHT;
            };
            return new Reservation(duplicate, null);
        }
        if (previous != null) recentEmails.remove(emailKey);
        if (recentEmails.size() >= maxRecentEmails) {
            Iterator<Map.Entry<String, Attempt>> entries = recentEmails.entrySet().iterator();
            while (entries.hasNext()) {
                if (!active(entries.next().getValue(), timestamp)) entries.remove();
            }
            if (recentEmails.size() >= maxRecentEmails) return new Reservation(Admission.FULL, null);
        }
        Attempt attempt = new Attempt(timestamp, State.IN_FLIGHT);
        recentEmails.put(emailKey, attempt);
        return new Reservation(Admission.SUBMIT, attempt);
    }

    private static boolean active(Attempt attempt, Instant now) {
        Duration cooldown = switch (attempt.state()) {
            case SUCCEEDED -> SUCCESS_COOLDOWN;
            case FAILED -> FAILURE_COOLDOWN;
            case IN_FLIGHT -> IN_FLIGHT_COOLDOWN;
        };
        return attempt.at().isAfter(now.minus(cooldown));
    }

    private synchronized void complete(String emailKey, Attempt original, State state) {
        if (recentEmails.get(emailKey) == original) {
            recentEmails.put(emailKey, new Attempt(now.get(), state));
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        String trimmed = s.trim();
        if (trimmed.isEmpty()) return null;
        return trimmed.length() <= max ? trimmed : trimmed.substring(0, max);
    }
}
