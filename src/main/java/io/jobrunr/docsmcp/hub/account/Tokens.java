package io.jobrunr.docsmcp.hub.account;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Token formats. Only SHA-256 hashes are stored.
 * <ul>
 *     <li>{@code jrc_read_…} / {@code jrc_operate_…}: connector token, lives in the customer's application config. The
 *     scope is part of the token itself, because the connector enforces it without asking the hub.</li>
 *     <li>{@code jra_…}: agent token, lives in the developer's MCP client config.</li>
 * </ul>
 */
public final class Tokens {

    public static final String CONNECTOR_READ_PREFIX = "jrc_read_";
    public static final String CONNECTOR_OPERATE_PREFIX = "jrc_operate_";
    public static final String AGENT_PREFIX = "jra_";

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private Tokens() {
    }

    public static String connectorToken(boolean operate) {
        return (operate ? CONNECTOR_OPERATE_PREFIX : CONNECTOR_READ_PREFIX) + random(40);
    }

    public static String agentToken() {
        return AGENT_PREFIX + random(40);
    }

    public static String random(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET[RANDOM.nextInt(ALPHABET.length)]);
        }
        return sb.toString();
    }

    public static String hash(String token) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String hint(String token) {
        return token.substring(token.length() - 4);
    }
}
