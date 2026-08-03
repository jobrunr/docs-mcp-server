package io.jobrunr.docsmcp;

import io.jobrunr.docsmcp.logging.LoggingToolCallbackProvider;
import io.jobrunr.docsmcp.logging.QueryLogClient;
import io.jobrunr.docsmcp.tools.DocsTools;
import io.jobrunr.docsmcp.trial.TrialTools;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableScheduling
public class DocsMcpApplication {

    public static void main(String[] args) {
        SpringApplication.run(DocsMcpApplication.class, args);
    }

    @Bean
    public ToolCallbackProvider docsToolCallbacks(
            DocsTools docsTools,
            TrialTools trialTools,
            QueryLogClient queryLogClient,
            ObjectMapper objectMapper,
            @Value("${log.api.body-truncate-bytes:2048}") int bodyTruncateBytes,
            @Value("${spring.ai.mcp.server.version:unknown}") String serverVersion) {
        ToolCallbackProvider delegate = MethodToolCallbackProvider.builder()
                .toolObjects(docsTools, trialTools)
                .build();
        return new LoggingToolCallbackProvider(delegate, queryLogClient, objectMapper, bodyTruncateBytes, serverVersion);
    }
}
