package io.jobrunr.docsmcp.testsupport;

import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Deterministic 32-dim embedding so tests don't need to download an ONNX model.
 * Quality is irrelevant — we only validate wiring.
 */
public final class DeterministicEmbeddingModel implements EmbeddingModel {

    @Override
    public float[] embed(String text) {
        float[] v = new float[32];
        CRC32 crc = new CRC32();
        for (int i = 0; i < v.length; i++) {
            crc.reset();
            crc.update((text + ":" + i).getBytes());
            v[i] = (crc.getValue() % 1000) / 1000f - 0.5f;
        }
        return v;
    }

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        List<Embedding> items = new ArrayList<>();
        int i = 0;
        for (String t : request.getInstructions()) {
            items.add(new Embedding(embed(t), i++));
        }
        return new EmbeddingResponse(items);
    }

    @Override
    public float[] embed(Document document) {
        return embed(document.getText());
    }
}
