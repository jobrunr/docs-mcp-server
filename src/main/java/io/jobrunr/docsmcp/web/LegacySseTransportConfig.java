package io.jobrunr.docsmcp.web;

import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerProperties;
import org.springframework.ai.mcp.server.webflux.transport.WebFluxSseServerTransportProvider;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;

/**
 * Keeps the deprecated HTTP+SSE transport (MCP protocol 2024-11-05) alive next to the
 * autoconfigured Streamable HTTP endpoint at {@code /mcp}.
 *
 * Spring AI's autoconfiguration wires exactly one transport — whichever
 * {@code spring.ai.mcp.server.protocol} selects — so the legacy pair
 * ({@code GET /sse} + {@code POST /mcp/message?sessionId=…}) has to be built by hand.
 * It runs as a second {@link McpSyncServer} inside this same application: same port,
 * same tools, same capabilities as the primary server, just a different wire protocol.
 *
 * Two things matter for correctness here:
 * <ul>
 *   <li>The transport provider must NOT be exposed as a bean. Spring AI's
 *       {@code mcpSyncServer} autoconfiguration injects a single
 *       {@code McpServerTransportProviderBase}; a second candidate breaks startup.</li>
 *   <li>Tools come from the shared {@code syncTools} bean and capabilities/serverInfo are
 *       read back off the primary server, so the two transports can never drift apart.</li>
 * </ul>
 *
 * This exists only so clients installed against the old {@code /sse} URL keep working.
 * Delete it — and the {@code mcp.legacy-sse.enabled} flag — once that traffic dies out.
 *
 * @see <a href="https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http">MCP: Streamable HTTP, Backward Compatibility</a>
 */
@Configuration
@ConditionalOnProperty(prefix = "mcp.legacy-sse", name = "enabled", havingValue = "true", matchIfMissing = true)
@SuppressWarnings("removal") // the deprecated transport is the whole point of this class
public class LegacySseTransportConfig {

    private static final Logger log = LoggerFactory.getLogger(LegacySseTransportConfig.class);

    /** Path already baked into every client installed before Streamable HTTP landed. */
    static final String SSE_ENDPOINT = "/sse";
    static final String MESSAGE_ENDPOINT = "/mcp/message";

    @Bean
    public LegacySseEndpoint legacySseEndpoint(
            @Qualifier("mcpServerJsonMapper") JsonMapper mcpServerJsonMapper,
            McpServerProperties serverProperties,
            McpSyncServer primaryServer,
            ObjectProvider<List<SyncToolSpecification>> tools,
            // Fly's proxy drops idle connections well before a minute; ping inside that window.
            @Value("${mcp.legacy-sse.keep-alive-interval:25s}") Duration keepAliveInterval) {

        WebFluxSseServerTransportProvider provider = WebFluxSseServerTransportProvider.builder()
                .jsonMapper(new JacksonMcpJsonMapper(mcpServerJsonMapper))
                .sseEndpoint(SSE_ENDPOINT)
                .messageEndpoint(MESSAGE_ENDPOINT)
                .keepAliveInterval(keepAliveInterval)
                .build();

        List<SyncToolSpecification> toolSpecifications = tools.stream().flatMap(List::stream).toList();

        McpSyncServer server = McpServer.sync(provider)
                .serverInfo(primaryServer.getServerInfo())
                .capabilities(primaryServer.getServerCapabilities())
                .instructions(serverProperties.getInstructions())
                .requestTimeout(serverProperties.getRequestTimeout())
                .tools(toolSpecifications)
                .build();

        log.info("Legacy MCP HTTP+SSE transport enabled at {} (messages: {}), {} tools, keep-alive {} "
                        + "— deprecated, prefer /mcp",
                SSE_ENDPOINT, MESSAGE_ENDPOINT, toolSpecifications.size(), keepAliveInterval);

        return new LegacySseEndpoint(provider, server);
    }

    @Bean
    public RouterFunction<?> legacySseRouterFunction(LegacySseEndpoint endpoint) {
        return endpoint.routerFunction();
    }

    /**
     * Holds the hand-built transport + server. Deliberately not a transport provider itself,
     * so it stays invisible to Spring AI's by-type transport injection.
     */
    public static final class LegacySseEndpoint implements AutoCloseable {

        private final WebFluxSseServerTransportProvider provider;
        private final McpSyncServer server;

        LegacySseEndpoint(WebFluxSseServerTransportProvider provider, McpSyncServer server) {
            this.provider = provider;
            this.server = server;
        }

        public RouterFunction<?> routerFunction() {
            return provider.getRouterFunction();
        }

        @Override
        public void close() {
            server.closeGracefully();
        }
    }
}
