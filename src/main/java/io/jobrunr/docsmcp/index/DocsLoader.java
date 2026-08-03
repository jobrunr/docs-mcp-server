package io.jobrunr.docsmcp.index;

import io.jobrunr.docsmcp.model.DocsCatalog;
import io.jobrunr.docsmcp.model.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class DocsLoader {

    private static final Logger log = LoggerFactory.getLogger(DocsLoader.class);

    private final WebClient webClient = WebClient.builder()
            .codecs(c -> c.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ReentrantLock loadLock = new ReentrantLock();

    private final String docsUrl;
    private final String manifestUrl;
    private final DocsRegistry registry;
    private final LuceneIndex luceneIndex;
    private final VectorIndex vectorIndex;

    private volatile String currentSha;

    public DocsLoader(
            @Value("${docs.url}") String docsUrl,
            @Value("${docs.manifest-url}") String manifestUrl,
            DocsRegistry registry,
            LuceneIndex luceneIndex,
            VectorIndex vectorIndex) {
        this.docsUrl = docsUrl;
        this.manifestUrl = manifestUrl;
        this.registry = registry;
        this.luceneIndex = luceneIndex;
        this.vectorIndex = vectorIndex;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        try {
            reload(true);
        } catch (Exception e) {
            log.warn("Initial docs load failed: {}", e.toString());
        }
    }

    @Scheduled(fixedRateString = "${docs.poll-interval:PT15M}")
    public void poll() {
        try {
            reload(false);
        } catch (Exception e) {
            log.warn("Scheduled docs reload failed: {}", e.toString());
        }
    }

    public void reload(boolean force) throws Exception {
        if (!loadLock.tryLock()) {
            log.info("Reload already in progress, skipping");
            return;
        }
        try {
            Manifest manifest = fetchManifest();
            if (!force && manifest.sha256() != null && manifest.sha256().equals(currentSha)) {
                log.debug("Docs unchanged (sha={}), skipping rebuild", manifest.sha256());
                return;
            }
            log.info("Loading docs catalog from {} (sha={}, count={})",
                    docsUrl, manifest.sha256(), manifest.count());
            DocsCatalog catalog = fetchCatalog();
            registry.set(catalog);
            luceneIndex.rebuild(catalog);
            vectorIndex.rebuild(catalog);
            currentSha = manifest.sha256();
            log.info("Reload complete: {} pages indexed", catalog.pages().size());
        } finally {
            loadLock.unlock();
        }
    }

    private Manifest fetchManifest() throws Exception {
        return mapper.readValue(fetchBody(manifestUrl, Duration.ofSeconds(10)), Manifest.class);
    }

    private DocsCatalog fetchCatalog() throws Exception {
        return mapper.readValue(fetchBody(docsUrl, Duration.ofSeconds(30)), DocsCatalog.class);
    }

    private String fetchBody(String url, Duration timeout) throws Exception {
        if (url.startsWith("file:")) {
            return Files.readString(Path.of(URI.create(url)));
        }
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .timeout(timeout)
                .block();
    }
}
