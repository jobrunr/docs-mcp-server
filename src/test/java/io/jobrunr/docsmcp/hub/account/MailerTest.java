package io.jobrunr.docsmcp.hub.account;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MailerTest {

    @Test
    void confirmationEmailEscapesSubmittedNameAndLinkAttributes() {
        HubProperties properties = new HubProperties("https://mcp.example.invalid", "JobRunr <noreply@example.invalid>",
                "", Duration.ofSeconds(25), Duration.ofSeconds(20), Duration.ofMinutes(30), Duration.ofDays(30));
        Account account = new Account("account-id", "recipient@example.invalid",
                "<a href=\"https://attacker.example.invalid\">Action needed</a>", "Example", null, null,
                null, Instant.now(), null, false);
        String link = "https://mcp.example.invalid/verify#abc\" onclick=\"evil()";

        String html = new Mailer(null, properties).body(account, link, true);

        assertThat(html).contains("&lt;a href=&quot;https://attacker.example.invalid&quot;&gt;Action needed&lt;/a&gt;");
        assertThat(html).doesNotContain("<a href=\"https://attacker.example.invalid\">");
        assertThat(html).contains("abc&quot; onclick=&quot;evil()");
        assertThat(html).doesNotContain("abc\" onclick=\"evil()");
    }
}
