package io.jobrunr.docsmcp.hub.account;

import java.time.Instant;

public record Account(
        String id,
        String email,
        String name,
        String company,
        String role,
        String useCase,
        String clusterId,
        Instant createdAt,
        Instant verifiedAt) {

    public String emailDomain() {
        int at = email.indexOf('@');
        return at >= 0 ? email.substring(at + 1) : null;
    }
}
