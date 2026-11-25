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
        Instant verifiedAt,
        boolean proEnabled) {

    public Account withClusterId(String clusterId) {
        return new Account(id, email, name, company, role, useCase, clusterId, createdAt, verifiedAt, proEnabled);
    }

    public String emailDomain() {
        int at = email.indexOf('@');
        return at >= 0 ? email.substring(at + 1) : null;
    }
}
