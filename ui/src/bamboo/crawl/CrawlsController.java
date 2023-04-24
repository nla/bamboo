package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.pandas.PandasInstance;
import bamboo.task.HeritrixJob;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import org.apache.commons.io.IOUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerMapping;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Collections;
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
    @PreAuthorize("hasRole('PANADMIN')")
    String index(@RequestParam(value = "page", defaultValue = "1") long page, Model model) {
        Pager<CrawlAndSeriesName> pager = bamboo.crawls.pager(page);
        model.addAttribute("crawls", pager.items);
        model.addAttribute("crawlsPager", pager);
        return "crawls/index";
    }

    @GetMapping(value = "/crawls/{id}")
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
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

    @GetMapping("/crawls/by-webrecorder-id/{id}")
    String findByWebrecorderCollectionId(@PathVariable("id") String id) {
        Crawl crawl = bamboo.crawls.getByWebrecorderCollectionId(id);
        return "redirect:/crawls/" + crawl.getId();
    }

    @GetMapping("/crawls/{id}/edit")
    @PreAuthorize("hasPermission(#id, 'Crawl', 'edit')")
    String edit(@PathVariable("id") long id, Model model) {
        Crawl crawl = bamboo.crawls.get(id);
        model.addAttribute("crawl", crawl);
        return "crawls/edit";
    }

    @PostMapping("/crawls/{id}/edit")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    String update(@PathVariable("id") long crawlId,
                  @RequestParam(value = "name") String name,
                  @RequestParam(value = "description", required = false) String description) {
        bamboo.crawls.update(crawlId, name, description);
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/crawls/{id}/warcs")
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
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
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
    String listArtifacts(@PathVariable("id") long id, Model model) {
        model.addAttribute("crawl", bamboo.crawls.get(id));
        model.addAttribute("artifacts", bamboo.crawls.listArtifacts(id));
        return "crawls/artifacts";
    }

    @GetMapping("/crawls/{id}/artifacts/{artifactId}/download")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'view')")
    void downloadArtifactById(@PathVariable("id") long crawlId,
                                     @PathVariable("artifactId") long artifactId,
                                     HttpServletRequest request,
                                     HttpServletResponse response) throws IOException {
        Artifact artifact = bamboo.crawls.getArtifact(artifactId);
        downloadArtifact(request, response, artifact);
    }

    @GetMapping("/crawls/{id}/artifacts/**")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'view')")
    void downloadArtifactByPath(@PathVariable("id") long crawlId,
                                HttpServletRequest request,
                                HttpServletResponse response) throws IOException {
        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String pattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        String relpath = new AntPathMatcher().extractPathWithinPattern(pattern, path);
        Artifact artifact = bamboo.crawls.getArtifactByRelpath(crawlId, relpath);
        downloadArtifact(request, response, artifact);
    }

    private void downloadArtifact(HttpServletRequest request, HttpServletResponse response, Artifact artifact) throws IOException {
        String filename = artifact.getRelpath().replaceFirst(".*/", "");
        response.setContentType(artifact.guessContentType());
        response.setContentLengthLong(artifact.getSize());
        response.setHeader("Content-Disposition", "filename=" + filename);
        String sha256 = artifact.getSha256();
        if (sha256 != null) {
            response.setHeader("Digest", "sha-256=" + Base64.getEncoder().encodeToString(Hex.decode(sha256)));
        }

        if (request.getMethod().equals("HEAD")) return;
        try (InputStream is = bamboo.crawls.openArtifactStream(artifact);
             OutputStream os = response.getOutputStream()) {
            IOUtils.copy(is, os);
        }
    }

    @PutMapping("/crawls/{id}/artifacts/**")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    @ResponseStatus(HttpStatus.CREATED)
    void putArtifactByPath(@PathVariable("id") long crawlId,
                                HttpServletRequest request) throws IOException {
        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String pattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        String relpath = new AntPathMatcher().extractPathWithinPattern(pattern, path);

        bamboo.crawls.get(crawlId);
        Artifact existing = bamboo.crawls.getArtifactByRelpathOrNull(crawlId, relpath);
        if (existing != null) throw new ResponseStatusException(HttpStatus.CONFLICT, "artifact already exists");
        bamboo.crawls.addArtifacts(crawlId, List.of(new NamedStream() {
            @Override
            public String name() {
                return relpath;
            }

            @Override
            public long length() {
                return request.getContentLengthLong();
            }

            @Override
            public InputStream openStream() throws IOException {
                return request.getInputStream();
            }
        }));
    }

    @GetMapping("/crawls/{id}/warcs/corrupt")
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
    String listCorruptWarcs(@PathVariable("id") long id,
                            @RequestParam(value = "page", defaultValue = "1") long page,
                            Model model) {
        Crawl crawl = bamboo.crawls.get(id);
        Pager<Warc> pager = bamboo.warcs.paginateWithCrawlIdAndState(page, id, Warc.CDX_ERROR);
        model.addAttribute("titlePrefix", "Corrupt");
        model.addAttribute("crawl", crawl);
        model.addAttribute("warcs", pager.items);
        model.addAttribute("warcsPager", pager);
        return "crawls/warcs/index";
    }

    @GetMapping("/crawls/{id}/warcs/download")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'view')")
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
    @PreAuthorize("hasPermission(#id, 'Crawl', 'view')")
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


    @GetMapping("/series/{seriesId}/upload")
    @PreAuthorize("hasPermission(#seriesId, 'Series', 'view')")
    public String seriesUploadForm(@PathVariable("seriesId") long seriesId, Model model) {
        return showNewForm(seriesId, model);
    }

    @PostMapping("/series/{seriesId}/upload")
    @PreAuthorize("hasPermission(#seriesId, 'Series', 'view')")
    public String seriesUpload(@PathVariable("seriesId") long seriesId,
                               Crawl crawl,
                               @RequestPart(value = "warcFile", required = false) MultipartFile[] warcFiles,
                               @RequestPart(value = "artifact", required = false) MultipartFile[] artifacts) throws IOException {
        crawl.setCrawlSeriesId(seriesId);
        return create(crawl, warcFiles, artifacts);
    }

    @GetMapping("/crawls/new")
    @PreAuthorize("hasRole('PANADMIN')")
    public String showNewForm(@RequestParam(value = "crawlSeries", defaultValue = "-1") long crawlSeriesId, Model model) {
        model.addAttribute("allCrawlSeries", bamboo.serieses.listAll());
        model.addAttribute("selectedCrawlSeriesId", crawlSeriesId);
        return "crawls/new";
    }

    @PostMapping("/crawls/new")
    @PreAuthorize("hasRole('PANADMIN')")
    public String create(Crawl crawl,
                         @RequestPart(value = "warcFile", required = false) MultipartFile[] warcFiles,
                         @RequestPart(value = "artifact", required = false) MultipartFile[] artifacts) throws IOException {
        if (warcFiles == null) warcFiles = new MultipartFile[0];
        if (artifacts == null) artifacts = new MultipartFile[0];
        long crawlId = bamboo.crawls.createFromStreams(crawl, NamedStream.of(warcFiles), NamedStream.of(artifacts));
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/import")
    @PreAuthorize("hasRole('PANADMIN')")
    public String showImportForm(@RequestParam(value = "crawlSeries", defaultValue = "-1") long crawlSeriesId, Model model) {
        model.addAttribute("allCrawlSeries", bamboo.serieses.listImportable());
        model.addAttribute("selectedCrawlSeriesId", crawlSeriesId);
        model.addAttribute("jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()));
        return "crawls/import";
    }

    @PostMapping("/import")
    @PreAuthorize("hasRole('PANADMIN')")
    public String performImport(@RequestParam("heritrixJob") String jobName,
                         @RequestParam("crawlSeriesId") long crawlSeriesId) {
        long crawlId = bamboo.crawls.importHeritrixCrawl(jobName, crawlSeriesId);
        return "redirect:/crawls/" + crawlId;
    }

    @GetMapping("/crawls/{crawlId}/warcs/upload")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    String uploadWarcsForm(@PathVariable("crawlId") long crawlId, Model model) {
        model.addAttribute("crawl", bamboo.crawls.get(crawlId));
        return "crawls/warcs/upload";
    }

    @PostMapping("/crawls/{crawlId}/warcs/upload")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    public String create(@PathVariable("crawlId") long crawlId, @RequestPart("warcFile") MultipartFile[] warcFiles) throws IOException {
        bamboo.crawls.addWarcs(crawlId, NamedStream.of(warcFiles));
        return "redirect:/crawls/" + crawlId + "/warcs";
    }

    @PutMapping("/crawls/{crawlId}/warcs/{filename}")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    @ResponseStatus(HttpStatus.CREATED)
    public void putWarc(@PathVariable("crawlId") long crawlId, @PathVariable("filename") String filename,
                          @RequestParam(name= "replaceCorrupt", defaultValue = "false") boolean replaceCorrupt,
                          HttpServletRequest request, HttpServletResponse response) throws IOException, NoSuchAlgorithmException {
        bamboo.crawls.get(crawlId); // ensure the crawl exists
        Warc existing = bamboo.warcs.getOrNullByCrawlIdAndFilename(crawlId, filename);
        if (existing != null) {
            if (!replaceCorrupt) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "File already exists");
            }
            boolean didReplace = bamboo.warcs.replaceCorruptBlob(existing, request.getInputStream(), request.getContentLengthLong());
            if (!didReplace) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "File already exists and appears valid");
            }
            return;
        }
        var warcs = bamboo.crawls.addWarcs(crawlId, List.of(new NamedStream() {
            @Override
            public String name() {
                return filename;
            }

            @Override
            public long length() {
                return request.getContentLengthLong();
            }

            @Override
            public InputStream openStream() throws IOException {
                return request.getInputStream();
            }
        }));
        long warcId = warcs.get(0).getId();
        var location = ServletUriComponentsBuilder.fromContextPath(request)
                .pathSegment("warcs", String.valueOf(warcId)).toUriString();
        response.setHeader("Location", location);
    }

    @DeleteMapping("/crawls/{crawlId}/warcs/{filename}")
    @PreAuthorize("hasPermission(#crawlId, 'Crawl', 'edit')")
    @ResponseStatus(HttpStatus.OK)
    public void deleteWarc(@PathVariable("crawlId") long crawlId, @PathVariable("filename") String filename) throws IOException {
        Warc warc = bamboo.warcs.getByFilename(filename);
        if (warc.getStateId() == Warc.DELETED) return;
        bamboo.cdxIndexer.deindexWarc(warc);
    }
}
