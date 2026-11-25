package io.jobrunr.docsmcp.index;

import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.transformers.TransformersEmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
public class EmbeddingConfig {

    @Bean
    public EmbeddingModel embeddingModel(
            @Value("${embedding.tokenizer-uri:}") String tokenizerUri,
            @Value("${embedding.model-uri:}") String modelUri) throws Exception {
        TransformersEmbeddingModel model = new TransformersEmbeddingModel();
        if (StringUtils.hasText(tokenizerUri)) model.setTokenizerResource(tokenizerUri);
        if (StringUtils.hasText(modelUri)) model.setModelResource(modelUri);
        model.afterPropertiesSet();
        return model;
    }
}
