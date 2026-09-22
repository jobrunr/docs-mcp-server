package io.jobrunr.docsmcp.hub.account;

import io.jobrunr.docsmcp.hub.HubProperties;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

/**
 * Sends the sign-in link, the same way the JobRunr Pro guided tour does: Spring's mail starter over SMTP
 * ({@code SPRING_MAIL_HOST}, {@code SPRING_MAIL_PORT}, {@code SPRING_MAIL_USERNAME}, {@code SPRING_MAIL_PASSWORD}),
 * from {@code noreply@jobrunr.io}, as an HTML mail with the logo. Without an SMTP host the link is written to the log,
 * which is only useful in local development.
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

    public boolean isConfigured() {
        return mailSender.getIfAvailable() != null;
    }

    public void sendSignInLink(Account account, String link, boolean firstTime) {
        JavaMailSender sender = mailSender.getIfAvailable();
        if (sender == null) {
            log.warn("No SMTP server configured (spring.mail.host). Sign-in link for {}: {}", account.email(), link);
            return;
        }
        try {
            MimeMessage mime = sender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mime, "UTF-8");
            helper.setFrom(properties.mailFrom());
            helper.setTo(account.email());
            helper.setSubject(firstTime ? "Confirm your email for JobRunr MCP" : "Your sign-in link for JobRunr MCP");
            helper.setText(body(account, link, firstTime), true);
            sender.send(mime);
            log.info("Sign-in link sent to {}", account.emailDomain());
        } catch (MessagingException e) {
            throw new IllegalStateException("Failed to send the sign-in email", e);
        }
    }

    private String body(Account account, String link, boolean firstTime) {
        String title = firstTime ? "Confirm your email" : "Sign in to JobRunr MCP";
        String intro = firstTime
                ? "Thanks for signing up for JobRunr MCP, " + account.name() + ". Confirm your email to get your connector and agent tokens."
                : "Here is your link to sign in to JobRunr MCP.";
        String button = firstTime ? "Confirm and get my tokens" : "Sign in to JobRunr MCP";
        String logoUrl = properties.publicUrl() + "/jobrunr-logo.png";
        long minutes = properties.magicLinkTtl().toMinutes();
        return """
                <!DOCTYPE html>
                <html xmlns="http://www.w3.org/1999/xhtml">
                <head>
                  <meta charset="UTF-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1.0">
                  <meta name="x-apple-disable-message-reformatting">
                </head>
                <body style="margin:0;padding:0;background:#f4f4f5;font-family:Arial,Helvetica,sans-serif;">
                <table width="100%%" cellpadding="0" cellspacing="0" role="presentation">
                  <tr><td align="center" bgcolor="#f4f4f5" style="padding:32px 16px;">
                    <table width="520" cellpadding="0" cellspacing="0" role="presentation" bgcolor="#ffffff" style="background:#ffffff;">
                      <tr>
                        <td bgcolor="#7952b3" style="background:#7952b3;padding:24px 32px;">
                          <img src="%s" alt="JobRunr" height="32" style="display:block;border:0;height:32px;">
                        </td>
                      </tr>
                      <tr>
                        <td style="padding:36px 32px 24px;">
                          <p style="margin:0 0 8px;font-size:18px;font-weight:bold;color:#18181b;">%s</p>
                          <p style="margin:0 0 28px;font-size:14px;color:#71717a;line-height:1.6;">
                            %s This link works once and expires in <strong>%d&nbsp;minutes</strong>.
                          </p>
                          <table cellpadding="0" cellspacing="0" role="presentation">
                            <tr>
                              <td bgcolor="#7952b3" style="background:#7952b3;">
                                <a href="%s" style="display:inline-block;color:#ffffff;text-decoration:none;padding:12px 28px;font-size:14px;font-weight:600;">%s</a>
                              </td>
                            </tr>
                          </table>
                          <p style="margin:28px 0 0;font-size:12px;color:#a1a1aa;line-height:1.6;">
                            Or copy this link into your browser:<br>
                            <a href="%s" style="color:#7952b3;word-break:break-word;">%s</a>
                          </p>
                          <p style="margin:16px 0 0;font-size:12px;color:#a1a1aa;">
                            If you didn't request this, you can safely ignore this email.
                          </p>
                        </td>
                      </tr>
                      <tr>
                        <td style="padding:16px 32px;border-top:1px solid #f4f4f5;">
                          <p style="margin:0;font-size:11px;color:#a1a1aa;text-align:center;">
                            &copy; JobRunr &nbsp;&middot;&nbsp; Background job processing for the JVM
                          </p>
                        </td>
                      </tr>
                    </table>
                  </td></tr>
                </table>
                </body>
                </html>
                """.formatted(logoUrl, title, intro, minutes, link, button, link, link);
    }
}
