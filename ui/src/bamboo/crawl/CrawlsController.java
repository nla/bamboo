package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.pandas.PandasInstance;
import bamboo.task.HeritrixJob;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerMapping;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

@Controller
public class CrawlsController {
    final Bamboo bamboo;

    public CrawlsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    @GetMapping("/crawls")
    String index(@RequestParam(value = "page", defaultValue = "1") long page, Model model) {
        Pager<CrawlAndSeriesName> pager = bamboo.crawls.pager(page);
        model.addAttribute("crawls", pager.items);
        model.addAttribute("crawlsPager", pager);
        return "crawls/index";
    }

    @GetMapping("/crawls/{id}")
    String show(@PathVariable("id") long id, Model model, HttpServletRequest request) {
        Crawl crawl = bamboo.crawls.get(id);
        CrawlStats stats = bamboo.crawls.stats(id);

        PandasInstance instance = null;
        if (crawl.getPandasInstanceId() != null && bamboo.pandas != null) {
            instance = bamboo.pandas.getInstance(crawl.getPandasInstanceId());
        }

        model.addAttribute("crawl", crawl);
        model.addAttribute("series", bamboo.serieses.get(crawl.getCrawlSeriesId()));
        model.addAttribute("warcsToBeCdxIndexed", stats.getWarcsToBeCdxIndexed());
        model.addAttribute("corruptWarcs", stats.getCorruptWarcs());
        model.addAttribute("descriptionHtml", Markdown.render(crawl.getDescription(), request.getRequestURI()));
        model.addAttribute("pandasInstance", instance);
        model.addAttribute("stats", stats);
        return "crawls/show";
    }

    @GetMapping("/instances/{instanceId}")
    String showByInstanceId(@PathVariable("instanceId") long instanceId) {
        Crawl crawl = bamboo.crawls.getByPandasInstanceIdOrNull(instanceId);
        if (crawl == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No crawl with instanceId " + instanceId);
        }
        return "redirect:/crawls/" + crawl.getId();
    }

    @GetMapping("/crawls/{id}/edit")
    String edit(@PathVariable("id") long id, Model model) {
        Crawl crawl = bamboo.crawls.get(id);
        model.addAttribute("crawl", crawl);
        return "crawls/edit";
    }

    @PostMapping("/crawls/{id}/edit")
    String update(@PathVariable("id") long crawlId,
                  @RequestParam(value = "name") String name,
                  @RequestParam(value = "description", required = false) String description) {
        bamboo.crawls.update(crawlId, name, description);
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/crawls/{id}/warcs")
    String listWarcs(@PathVariable("id") long id,
                     @RequestParam(value = "page", defaultValue = "1") long page,
                     @RequestParam(value = "format", defaultValue = "html") String format,
                     Model model, HttpServletResponse response) throws IOException {
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlId(page, id);

        if ("ids".equals(format)) {
            response.setContentType("text/plain");
            response.setHeader("Total", String.valueOf(pager.totalItems));
            try (Writer w = new OutputStreamWriter(response.getOutputStream())) {
                for (Warc warc: pager.items) {
                    w.write(warc.getId() + "\n");
                }
            }
            return null;
        }

        model.addAttribute("crawl", crawl);
        model.addAttribute("warcs", pager.items);
        model.addAttribute("warcsPager", pager);
        return "crawls/warcs/index";
    }

    @GetMapping("/crawls/{id}/artifacts")
    String listArtifacts(@PathVariable("id") long id, Model model) {
        model.addAttribute("crawl", bamboo.crawls.get(id));
        model.addAttribute("artifacts", bamboo.crawls.listArtifacts(id));
        return "crawls/artifacts";
    }

    @GetMapping("/crawls/{id}/artifacts/{artifactId}/download")
    void downloadArtifactById(@PathVariable("id") long crawlId,
                                     @PathVariable("artifactId") long artifactId,
                                     HttpServletResponse response) throws IOException {
        Artifact artifact = bamboo.crawls.getArtifact(artifactId);
        downloadArtifact(response, artifact);
    }

    @GetMapping("/crawls/{id}/artifacts/**")
    void downloadArtifactByPath(@PathVariable("id") long crawlId,
                                HttpServletRequest request,
                                HttpServletResponse response) throws IOException {
        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String pattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        String relpath = new AntPathMatcher().extractPathWithinPattern(pattern, path);
        Artifact artifact = bamboo.crawls.getArtifactByRelpath(crawlId, relpath);
        downloadArtifact(response, artifact);
    }

    private void downloadArtifact(HttpServletResponse response, Artifact artifact) throws IOException {
        String filename = artifact.getRelpath().replaceFirst(".*/", "");
        response.setContentType(artifact.guessContentType());
        response.setHeader("Content-Disposition", "filename=" + filename);
        try (InputStream is = bamboo.crawls.openArtifactStream(artifact);
             OutputStream os = response.getOutputStream()) {
            IOUtils.copy(is, os);
        }
    }

    @GetMapping("/crawls/{id}/warcs/corrupt")
    String listCorruptWarcs(@PathVariable("id") long id,
                            @RequestParam(value = "page", defaultValue = "1") long page,
                            Model model) {
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlIdAndState(page, id, Warc.CDX_ERROR);
        model.addAttribute("titlePrefix", "Corrupt");
        model.addAttribute("crawl", crawl);
        model.addAttribute("warcs", pager.items);
        model.addAttribute("warcsPager", pager);
        return "crawls/warcs";
    }

    @GetMapping("/crawls/{id}/warcs/download")
    void downloadWarcs(@PathVariable("id") long crawlId, HttpServletResponse response) throws IOException {
        List<Warc> warcs = bamboo.warcs.findByCrawlId(crawlId);
        if (warcs.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No warcs found");
        }

        response.setContentType("application/zip");
        response.setHeader("Content-Disposition", "attachment; filename=crawl-" + crawlId + ".zip");

        try (ZipOutputStream zip = new ZipOutputStream(response.getOutputStream())) {
            for (Warc warc : warcs) {
                writeZipEntry(zip, "crawl-" + crawlId + "/" + warc.getFilename(), warc.getPath());
            }
        }
    }

    private void writeZipEntry(ZipOutputStream zip, String entryName, Path sourceFile) throws IOException {
        long size = Files.size(sourceFile);

        ZipEntry entry = new ZipEntry(entryName);
        entry.setLastModifiedTime(Files.getLastModifiedTime(sourceFile));
        entry.setSize(size);
        entry.setCompressedSize(size);
        entry.setCrc(crc32(sourceFile));

        zip.setMethod(ZipOutputStream.STORED);
        zip.putNextEntry(entry);
        Files.copy(sourceFile, zip);
        zip.closeEntry();
    }

    private long crc32(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file)) {
            CRC32 crc32 = new CRC32();
            ByteBuffer buffer = ByteBuffer.allocate(16384);
            while (channel.read(buffer) >= 0) {
                buffer.flip();
                crc32.update(buffer);
                buffer.clear();
            }
            return crc32.getValue();
        }
    }

    @GetMapping(value = "/crawls/{id}/warcs/reports", produces = "text/plain")
    @ResponseBody
    String listReports(@PathVariable("id") long id) {
        Crawl crawl = bamboo.crawls.get(id);
        StringBuilder out = new StringBuilder();
        Path bundle = crawl.getPath().resolve("crawl-bundle.zip");
        try (ZipFile zip = new ZipFile(bundle.toFile())) {
            for (ZipEntry entry : Collections.list(zip.entries())) {
                out.append(entry.getName()).append("\n");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toString();
    }

    @GetMapping("/crawls/new")
    public String showNewForm(@RequestParam(value = "crawlSeries", defaultValue = "-1") long crawlSeriesId, Model model) {
        model.addAttribute("allCrawlSeries", bamboo.serieses.listAll());
        model.addAttribute("selectedCrawlSeriesId", crawlSeriesId);
        return "crawls/new";
    }

    @PostMapping("/crawls/new")
    public String create(Crawl crawl,
                         @RequestPart(value = "warcFile", required = false) MultipartFile[] warcFiles,
                         @RequestPart(value = "artifact", required = false) MultipartFile[] artifacts) throws IOException {
        long crawlId = bamboo.crawls.create(crawl, warcFiles, artifacts);
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/import")
    public String showImportForm(@RequestParam(value = "crawlSeries", defaultValue = "-1") long crawlSeriesId, Model model) {
        model.addAttribute("allCrawlSeries", bamboo.serieses.listImportable());
        model.addAttribute("selectedCrawlSeriesId", crawlSeriesId);
        model.addAttribute("jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()));
        return "crawls/import";
    }

    @PostMapping("/import")
    public String performImport(@RequestParam("heritrixJob") String jobName,
                         @RequestParam("crawlSeriesId") long crawlSeriesId) {
        long crawlId = bamboo.crawls.importHeritrixCrawl(jobName, crawlSeriesId);
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/crawls/{crawlId}/warcs/upload")
    String uploadWarcsForm(@PathVariable("crawlId") long crawlId, Model model) {
        model.addAttribute("crawl", bamboo.crawls.get(crawlId));
        return "crawls/warcs/upload";
    }

    @PostMapping("/crawls/{crawlId}/warcs/upload")
    public String create(@PathVariable("crawlId") long crawlId, @RequestPart("warcFile") MultipartFile[] warcFiles) throws IOException {
        bamboo.crawls.addWarcs(crawlId, warcFiles);
        return "redirect:/crawls/" + crawlId + "/warcs";
    }

}
