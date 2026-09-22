package io.jobrunr.docsmcp.hub;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Settings of the ops hub: the part of this server that lets agents reach a customer's JobRunr cluster through the
 * connector inside their JVM.
 *
 * @param publicUrl       base url used in emails and setup snippets
 * @param mailFrom        sender of the sign-in emails
 * @param leadsWebhookUrl optional n8n webhook that receives signups, first connections and Pro-feature attempts
 * @param pollHold        how long a connector long-poll is held open when there is nothing to do
 * @param requestTimeout  how long an agent waits for the connected instance to answer one dashboard request
 * @param magicLinkTtl    lifetime of a sign-in link
 * @param sessionTtl      lifetime of a browser session on the account page
 */
@ConfigurationProperties(prefix = "hub")
public record HubProperties(
        String publicUrl,
        String mailFrom,
        String leadsWebhookUrl,
        Duration pollHold,
        Duration requestTimeout,
        Duration magicLinkTtl,
        Duration sessionTtl) {
}
