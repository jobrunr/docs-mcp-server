package io.jobrunr.docsmcp.index;

import tools.jackson.databind.ObjectMapper;
import io.jobrunr.docsmcp.model.DocsCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneIndexTest {

    private static DocsCatalog catalog;

    @BeforeAll
    static void load() throws Exception {
        try (InputStream in = LuceneIndexTest.class.getResourceAsStream("/sample-docs.json")) {
            assertThat(in).isNotNull();
            catalog = new ObjectMapper().readValue(in, DocsCatalog.class);
        }
        assertThat(catalog.pages()).isNotEmpty();
    }

    @Test
    void buildsAndReturnsTopResultForKeywordQuery() {
        LuceneIndex index = new LuceneIndex();
        index.rebuild(catalog);
        assertThat(index.isReady()).isTrue();

        List<LuceneIndex.Result> hits = index.search("recurring jobs", 5);
        assertThat(hits).isNotEmpty();
        assertThat(hits.get(0).path()).contains("recurring-jobs");
        assertThat(hits.get(0).snippet()).contains("**");
    }

    @Test
    void returnsEmptyForBlankQuery() {
        LuceneIndex index = new LuceneIndex();
        index.rebuild(catalog);
        assertThat(index.search("", 5)).isEmpty();
        assertThat(index.search(null, 5)).isEmpty();
    }

    @Test
    void boostsTitleMatches() {
        LuceneIndex index = new LuceneIndex();
        index.rebuild(catalog);
        List<LuceneIndex.Result> hits = index.search("dashboard", 3);
        assertThat(hits).isNotEmpty();
        assertThat(hits.get(0).title().toLowerCase()).contains("dashboard");
    }
}
