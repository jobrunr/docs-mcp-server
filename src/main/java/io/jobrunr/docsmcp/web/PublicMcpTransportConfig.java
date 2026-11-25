package io.jobrunr.docsmcp.web;

import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerStreamableHttpProperties;
import org.springframework.ai.mcp.server.webflux.transport.WebFluxStreamableServerTransportProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

/**
 * Sets explicit limits on the public MCP transport. Spring AI's default WebFlux
 * auto-configuration does not configure the SDK's session cap or idle eviction.
 */
@Configuration(proxyBeanMethods = false)
public class PublicMcpTransportConfig {

    @Bean
    public WebFluxStreamableServerTransportProvider webFluxStreamableServerTransportProvider(
            @Qualifier("mcpServerJsonMapper") JsonMapper jsonMapper,
            McpServerStreamableHttpProperties properties,
            @Value("${mcp.sessions.max:2000}") long maxSessions,
            @Value("${mcp.sessions.idle-timeout:PT1H}") Duration sessionIdleTimeout) {
        return WebFluxStreamableServerTransportProvider.builder()
                .jsonMapper(new JacksonMcpJsonMapper(jsonMapper))
                .messageEndpoint(properties.getMcpEndpoint())
                .keepAliveInterval(properties.getKeepAliveInterval())
                .disallowDelete(properties.isDisallowDelete())
                .maxSessions(maxSessions)
                .sessionIdleTimeout(sessionIdleTimeout)
                .build();
    }
}
