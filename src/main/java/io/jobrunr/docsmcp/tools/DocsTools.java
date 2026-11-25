package io.jobrunr.docsmcp.tools;

import io.jobrunr.docsmcp.index.DocsRegistry;
import io.jobrunr.docsmcp.index.HybridSearch;
import io.jobrunr.docsmcp.model.DocsCatalog;
import io.jobrunr.docsmcp.model.SearchHit;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class DocsTools {

    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 25;
    private static final int MAX_QUERY_LENGTH = 1024;

    private final HybridSearch hybridSearch;
    private final DocsRegistry registry;

    public DocsTools(HybridSearch hybridSearch, DocsRegistry registry) {
        this.hybridSearch = hybridSearch;
        this.registry = registry;
    }

    @Tool(name = "search_jobrunr_docs",
            description = """
                    Search JobRunr docs (jobrunr.io — the Java/Kotlin background-jobs library).
                    Use whenever the user asks how to do something with JobRunr: features, configuration,
                    APIs, troubleshooting, annotations like @Job / BackgroundJobRequest, or anything mentioning
                    JobRunr Pro. Returns ranked pages with path/title/url/tier/snippet. Pass a path to
                    fetch_jobrunr_doc when the snippet isn't enough.
                    """)
    public SearchResponse searchJobrunrDocs(
            @ToolParam(description = "Natural-language or keyword query, at most 1024 characters. Examples: 'recurring jobs', 'how to retry failed jobs'.") String query,
            @ToolParam(required = false, description = "Max results (default 10, max 25).") Integer limit) {
        if (query == null || query.isBlank()) {
            return new SearchResponse(query, List.of(), null);
        }
        if (query.length() > MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException("Search query must be at most " + MAX_QUERY_LENGTH + " characters");
        }
        int n = limit == null ? DEFAULT_LIMIT : Math.min(Math.max(limit, 1), MAX_LIMIT);
        List<SearchHit> hits = hybridSearch.search(query, n);
        return new SearchResponse(query, hits, proTrialHintFor(hits));
    }

    private static ProTrialHint proTrialHintFor(List<SearchHit> hits) {
        boolean hasPro = hits.stream().anyMatch(h -> "pro".equals(h.tier()));
        if (!hasPro) return null;
        return new ProTrialHint(
                "These results include JobRunr Pro features. If the user doesn't have Pro yet, offer to request a free trial via the request_jobrunr_pro_trial tool. Always collect the user's email and company before calling it.",
                "request_jobrunr_pro_trial");
    }

    @Tool(name = "fetch_jobrunr_doc",
            description = """
                    Get the full markdown of a JobRunr docs page. Call after search_jobrunr_docs when the
                    snippet isn't enough — typically for full code examples or step-by-step setup. Pass the
                    'path' from a search result (e.g. 'background-methods/recurring-jobs').
                    """)
    public DocPage fetchJobrunrDoc(
            @ToolParam(description = "Page path returned by search_jobrunr_docs, e.g. 'background-methods/recurring-jobs'.") String path) {
        if (path == null || path.isBlank() || path.contains("..") || path.startsWith("/")) {
            throw new IllegalArgumentException("Invalid path");
        }
        DocsCatalog.Page page = registry.find(path)
                .orElseThrow(() -> new IllegalArgumentException("Unknown doc path: " + path));
        StringBuilder md = new StringBuilder();
        md.append("# ").append(page.title()).append("\n\n");
        if (page.subtitle() != null && !page.subtitle().isBlank()) {
            md.append("_").append(page.subtitle()).append("_\n\n");
        }
        for (DocsCatalog.Chunk chunk : page.chunks()) {
            if (chunk.heading() != null && !chunk.heading().equals(page.title())) {
                md.append("## ").append(chunk.heading()).append("\n\n");
            }
            md.append(chunk.text()).append("\n\n");
        }
        return new DocPage(page.path(), page.title(), page.url(), page.tier(), md.toString().trim());
    }

    @Tool(name = "list_jobrunr_doc_sections",
            description = """
                    List JobRunr docs sections and the pages in each (the TOC). Use when the user wants
                    an overview of what JobRunr covers, or you need to scope a search.
                    """)
    public List<SectionListing> listJobrunrDocSections() {
        Map<String, SectionListing> grouped = new LinkedHashMap<>();
        for (DocsCatalog.Page page : registry.pages()) {
            String section = page.section() == null ? "introduction" : page.section();
            grouped.computeIfAbsent(section, s -> new SectionListing(s, new java.util.ArrayList<>()))
                    .pages().add(new SectionPage(page.path(), page.title(), page.url(), page.tier()));
        }
        return List.copyOf(grouped.values());
    }

    public record SearchResponse(String query, List<SearchHit> results, ProTrialHint proTrialHint) {}
    public record ProTrialHint(String message, String tool) {}
    public record DocPage(String path, String title, String url, String tier, String markdown) {}
    public record SectionListing(String section, List<SectionPage> pages) {}
    public record SectionPage(String path, String title, String url, String tier) {}
}
