package com.learn.aisagaagent.config;

import dev.langchain4j.mcp.McpToolProvider;
import dev.langchain4j.mcp.client.DefaultMcpClient;
import dev.langchain4j.mcp.client.McpClient;
import dev.langchain4j.mcp.client.transport.http.HttpMcpTransport;
import dev.langchain4j.mcp.client.transport.http.StreamableHttpMcpTransport;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class McpClientConfig {

    @Value("${mcp.order-service-url}")
    private String orderServiceUrl;
    @Value("${mcp.payment-service-url}")
    private String paymentServiceUrl;
    @Value("${mcp.inventory-service-url}")
    private String inventoryServiceUrl;
    @Value("${mcp.product-validation-url}")
    private String productValidationUrl;
    @Value("${mcp.orchestrator-url}")
    private String orchestratorUrl;
    @Value("${security.api-key:}")
    private String apiKey;

    @Bean
    public McpToolProvider mcpToolProvider() {
        return McpToolProvider.builder()
                .mcpClients(List.of(
                        buildClient(orderServiceUrl),
                        buildClient(paymentServiceUrl),
                        buildClient(inventoryServiceUrl),
                        buildClient(productValidationUrl)
                        //buildClient(orchestratorUrl)
                        )
                )
                .build();
    }

    private McpClient buildClient(String sseUrl) {
        return new DefaultMcpClient.Builder()
                .transport(httpTransport(sseUrl))
                .build();
    }

    private HttpMcpTransport httpTransport(String sseUrl) {
        var builder = new HttpMcpTransport.Builder()
                .sseUrl(sseUrl)
                .logResponses(true)
                .logRequests(true);
        if (apiKey != null && !apiKey.isBlank()) {
            builder.customHeaders(Map.of("X-API-Key", apiKey));
        }
        return builder.build();
    }


    private McpClient buildClient2(String mcpUrl) {
        return new DefaultMcpClient.Builder()
                .key(mcpUrl)
                .transport(new StreamableHttpMcpTransport.Builder()
                        .url(mcpUrl)           // aponta para /mcp, não /sse
                        .logRequests(false)
                        .logResponses(false)
                        .build())
                .build();
    }
}
