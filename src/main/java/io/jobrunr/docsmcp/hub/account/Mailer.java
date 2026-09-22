package io.jobrunr.docsmcp.hub.account;

import io.jobrunr.docsmcp.hub.HubProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Sends the sign-in link. Mail goes out over SMTP when {@code spring.mail.host} is configured; without it (local
 * development) the link is written to the log instead.
 */
@Component
public class Mailer {

    private static final Logger log = LoggerFactory.getLogger(Mailer.class);

    private final ObjectProvider<JavaMailSender> mailSender;
    private final HubProperties properties;

    public Mailer(ObjectProvider<JavaMailSender> mailSender, HubProperties properties) {
        this.mailSender = mailSender;
        this.properties = properties;
    }

    public void sendSignInLink(Account account, String link, boolean firstTime) {
        JavaMailSender sender = mailSender.getIfAvailable();
        if (sender == null) {
            log.warn("No SMTP server configured (spring.mail.host). Sign-in link for {}: {}", account.email(), link);
            return;
        }
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(properties.mailFrom());
        message.setTo(account.email());
        message.setSubject(firstTime ? "Confirm your email for JobRunr MCP" : "Your JobRunr MCP sign-in link");
        message.setText("""
                Hi %s,

                %s

                %s

                The link works once and expires in %d minutes. If you did not ask for it, you can ignore this email.

                The JobRunr team
                """.formatted(
                account.name(),
                firstTime ? "Thanks for signing up for JobRunr MCP. Confirm your email to get your tokens:" : "Here is your link to sign in to JobRunr MCP:",
                link,
                properties.magicLinkTtl().toMinutes()));
        sender.send(message);
    }
}
