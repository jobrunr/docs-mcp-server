package io.jobrunr.docsmcp.tools;

import tools.jackson.databind.ObjectMapper;
import io.jobrunr.docsmcp.index.DocsRegistry;
import io.jobrunr.docsmcp.index.HybridSearch;
import io.jobrunr.docsmcp.index.LuceneIndex;
import io.jobrunr.docsmcp.index.VectorIndex;
import io.jobrunr.docsmcp.model.DocsCatalog;
import io.jobrunr.docsmcp.model.SearchHit;
import io.jobrunr.docsmcp.testsupport.DeterministicEmbeddingModel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocsToolsTest {

    private static DocsCatalog catalog;
    private static DocsTools tools;
    private static DocsRegistry registry;

    @BeforeAll
    static void setup() throws Exception {
        try (InputStream in = DocsToolsTest.class.getResourceAsStream("/sample-docs.json")) {
            assertThat(in).isNotNull();
            catalog = new ObjectMapper().readValue(in, DocsCatalog.class);
        }
        registry = new DocsRegistry();
        registry.set(catalog);

        LuceneIndex lucene = new LuceneIndex();
        lucene.rebuild(catalog);

        VectorIndex vector = new VectorIndex(new DeterministicEmbeddingModel());
        vector.rebuild(catalog);

        HybridSearch hybrid = new HybridSearch(lucene, vector, 60, 50);
        tools = new DocsTools(hybrid, registry);
    }

    @Test
    void searchReturnsRankedResults() {
        DocsTools.SearchResponse resp = tools.searchJobrunrDocs("recurring jobs", 5);
        assertThat(resp.results()).isNotEmpty();
        assertThat(resp.results().get(0).path()).contains("recurring-jobs");
        SearchHit top = resp.results().get(0);
        assertThat(top.url()).startsWith("https://www.jobrunr.io/en/documentation/");
        assertThat(top.snippet()).isNotBlank();
    }

    @Test
    void searchEmptyQueryReturnsNoResults() {
        DocsTools.SearchResponse resp = tools.searchJobrunrDocs("", 5);
        assertThat(resp.results()).isEmpty();
        assertThat(resp.proTrialHint()).isNull();
    }

    @Test
    void searchAttachesProTrialHintWhenProResultsPresent() {
        DocsTools.SearchResponse resp = tools.searchJobrunrDocs("priority queues", 5);
        assertThat(resp.results()).isNotEmpty();
        assertThat(resp.results()).anyMatch(h -> "pro".equals(h.tier()));
        assertThat(resp.proTrialHint()).isNotNull();
        assertThat(resp.proTrialHint().tool()).isEqualTo("request_jobrunr_pro_trial");
        assertThat(resp.proTrialHint().message()).containsIgnoringCase("trial");
    }

    @Test
    void searchOmitsProTrialHintWhenOnlyOssResults() {
        DocsTools.SearchResponse resp = tools.searchJobrunrDocs("Jackson serialization", 3);
        assertThat(resp.results()).isNotEmpty();
        assertThat(resp.results()).allMatch(h -> "oss".equals(h.tier()));
        assertThat(resp.proTrialHint()).isNull();
    }

    @Test
    void fetchReturnsMarkdown() {
        DocsTools.DocPage page = tools.fetchJobrunrDoc("background-methods/recurring-jobs");
        assertThat(page.markdown()).startsWith("# ");
        assertThat(page.url()).contains("recurring-jobs");
    }

    @Test
    void fetchRejectsPathTraversal() {
        assertThatThrownBy(() -> tools.fetchJobrunrDoc("../etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tools.fetchJobrunrDoc("/absolute"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fetchUnknownPathThrows() {
        assertThatThrownBy(() -> tools.fetchJobrunrDoc("does-not-exist"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void listSectionsGroupsPages() {
        List<DocsTools.SectionListing> sections = tools.listJobrunrDocSections();
        assertThat(sections).isNotEmpty();
        assertThat(sections.stream().map(DocsTools.SectionListing::section))
                .contains("background-methods", "configuration");
    }

}
