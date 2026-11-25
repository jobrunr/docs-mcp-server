package io.jobrunr.docsmcp.hub.account;

import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaMigrationTest {

    @Test
    void existingAccountsAndTokensSurviveAdditiveSchemaChanges() throws Exception {
        String url = "jdbc:h2:mem:legacy-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        String oldAgentToken = "jra_legacy_old";
        String newAgentToken = "jra_legacy_new";
        String connectorToken = "jrc_read_legacy";
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE hub_accounts (
                      id VARCHAR(36) PRIMARY KEY, email VARCHAR(320) NOT NULL UNIQUE, name VARCHAR(200) NOT NULL,
                      company VARCHAR(200) NOT NULL, job_role VARCHAR(200), use_case VARCHAR(1000),
                      cluster_id VARCHAR(64), created_at TIMESTAMP NOT NULL, verified_at TIMESTAMP
                    )""");
            statement.execute("""
                    CREATE TABLE hub_tokens (
                      id VARCHAR(36) PRIMARY KEY, account_id VARCHAR(36) NOT NULL, kind VARCHAR(16) NOT NULL,
                      scope VARCHAR(16), token_hash VARCHAR(64) NOT NULL UNIQUE, hint VARCHAR(8) NOT NULL,
                      created_at TIMESTAMP NOT NULL, revoked_at TIMESTAMP
                    )""");
            statement.execute("INSERT INTO hub_accounts (id,email,name,company,cluster_id,created_at) VALUES " +
                    "('legacy-account','person@example.invalid','Person','Example','legacy-cluster',TIMESTAMP '2026-01-01 00:00:00')");
            statement.execute("INSERT INTO hub_tokens (id,account_id,kind,scope,token_hash,hint,created_at) VALUES " +
                    "('old-agent','legacy-account','agent',NULL,'" + Tokens.hash(oldAgentToken) + "','aaaa',TIMESTAMP '2026-01-01 00:00:00')");
            statement.execute("INSERT INTO hub_tokens (id,account_id,kind,scope,token_hash,hint,created_at) VALUES " +
                    "('new-agent','legacy-account','agent',NULL,'" + Tokens.hash(newAgentToken) + "','bbbb',TIMESTAMP '2026-01-02 00:00:00')");
            statement.execute("INSERT INTO hub_tokens (id,account_id,kind,scope,token_hash,hint,created_at) VALUES " +
                    "('connector','legacy-account','connector','read','" + Tokens.hash(connectorToken) + "','cccc',TIMESTAMP '2026-01-02 00:00:00')");

            runCurrentSchema(connection);
            statement.execute("""
                    INSERT INTO hub_instances (account_id, cluster_id, jobrunr_version, snapshot_at)
                    VALUES ('legacy-account', 'legacy-cluster', '8.0.0', TIMESTAMP '2026-01-02 00:00:00')
                    """);
            runCurrentSchema(connection); // startup SQL must remain safe on a second deployment

            try (var account = statement.executeQuery("SELECT is_pro FROM hub_accounts WHERE id = 'legacy-account'")) {
                assertThat(account.next()).isTrue();
                assertThat(account.getBoolean("is_pro")).isFalse();
            }
            try (var slots = statement.executeQuery("SELECT kind, token_hash FROM hub_current_tokens WHERE account_id = 'legacy-account' ORDER BY kind")) {
                assertThat(slots.next()).isTrue();
                assertThat(slots.getString("kind")).isEqualTo("agent");
                assertThat(slots.getString("token_hash")).isEqualTo(Tokens.hash(newAgentToken));
                assertThat(slots.next()).isTrue();
                assertThat(slots.getString("kind")).isEqualTo("connector");
                assertThat(slots.getString("token_hash")).isEqualTo(Tokens.hash(connectorToken));
                assertThat(slots.next()).isFalse();
            }
            var store = new AccountStore(JdbcClient.create(new DriverManagerDataSource(url, "sa", "")));
            assertThat(store.findActiveToken(newAgentToken, AccountStore.KIND_AGENT)).isPresent();
            assertThat(store.findActiveToken(connectorToken, AccountStore.KIND_CONNECTOR)).isPresent();
            assertThat(store.findActiveToken(oldAgentToken, AccountStore.KIND_AGENT)).isEmpty();
            store.setProEnabled("legacy-account", true);
            assertThat(store.findById("legacy-account").orElseThrow().proEnabled()).isTrue();
            assertThat(store.findInstances("legacy-account")).hasSize(1);
            assertThat(store.findInstance("legacy-account", "legacy-cluster").orElseThrow().jobrunrVersion()).isEqualTo("8.0.0");
        }
    }

    private static void runCurrentSchema(java.sql.Connection connection) throws Exception {
        try (var schema = new InputStreamReader(
                SchemaMigrationTest.class.getResourceAsStream("/schema.sql"), StandardCharsets.UTF_8)) {
            RunScript.execute(connection, schema);
        }
    }
}
