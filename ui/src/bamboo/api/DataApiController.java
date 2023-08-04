package bamboo.api;

import bamboo.app.Bamboo;
import bamboo.core.NotFoundException;
import bamboo.crawl.Crawl;
import bamboo.crawl.Warc;
import bamboo.crawl.WarcsController;
import bamboo.task.TextCache;
import com.drew.metadata.mov.atoms.Atom;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

@Controller
@ConditionalOnProperty({"data_api.credentials", "data_api.allowed_series_ids"})
public class DataApiController {
    private final Bamboo wa;
    private final WarcsController warcsController;
    @Value("${data_api.allowed_series_ids}")
    private Set<Long> allowedSeriesIds;
    @Value("${data_api.credentials}")
    private Set<String> credentials;
    @Value("${data_api.base_url}")
    private String dataApiBaseUrl;
    private AtomicInteger solrQueriesInFlight = new AtomicInteger();

    public DataApiController(Bamboo wa, WarcsController warcsController) {
        this.wa = wa;
        this.warcsController = warcsController;
    }

    public static class MissingCredentialsException extends Exception {
        public MissingCredentialsException(String message) {
            super(message);
        }
    }

    public static class AccessDeniedException extends Exception {
        public AccessDeniedException(String message) {
            super(message);
        }
    }

    @ExceptionHandler(MissingCredentialsException.class)
    @ResponseBody
    public Error handleMissingCredentialsException(MissingCredentialsException e, HttpServletResponse response) {
        response.setHeader("WWW-Authenticate", "Basic");
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        return new Error(e);
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseBody
    public Error handleNotFoundException(NotFoundException e, HttpServletResponse response) {
        response.setStatus(404);
        return new Error(e);
    }

    @ExceptionHandler(AccessDeniedException.class)
    @ResponseBody
    public Error handleAccessDeniedException(Exception e, HttpServletResponse response) {
        response.setStatus(403);
        return new Error(e);
    }

    public record Error(String exception, String message) {
        public Error(Exception e) {
            this(e.getClass().getSimpleName(), e.getMessage());
        }
    }

    private void enforceAgwaCredentials(HttpServletRequest request) throws MissingCredentialsException, AccessDeniedException {
        String authorization = request.getHeader("Authorization");
        if (authorization == null) {
            throw new MissingCredentialsException("Missing credentials");
        }
        if (StringUtils.startsWithIgnoreCase(authorization, "basic ")) {
            var credentials = new String(Base64.getDecoder().decode(authorization.substring(6)), UTF_8);
            if (this.credentials.contains(credentials)) {
                return; // OK
            }
        }
        throw new AccessDeniedException("Access denied");
    }

    private void enforceAgwaCrawl(Crawl crawl) throws AccessDeniedException {
        if (!allowedSeriesIds.contains(crawl.getCrawlSeriesId())) {
            throw new AccessDeniedException("Not an AGWA crawl");
        }
    }

    @GetMapping(value = "/data/crawls/{crawlId}/warcs", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ArrayList<WarcData> listWarcsByCrawl(@PathVariable long crawlId,
                                                @RequestParam(value = "page", defaultValue = "1") int page,
                                                @RequestParam(value = "size", defaultValue = "10000") int pageSize,
                                                HttpServletRequest request,
                                                UriComponentsBuilder uriBuilder) throws AccessDeniedException, MissingCredentialsException {
        enforceAgwaCredentials(request);
        var crawl = wa.crawls.get(crawlId);
        enforceAgwaCrawl(crawl);
        var pager = wa.warcs.paginateWithCrawlId(page, crawlId, pageSize);
        var list = new ArrayList<WarcData>();
        String baseUrl = dataApiBaseUrl;
        if (baseUrl == null) {
            baseUrl = uriBuilder.cloneBuilder().path("/data").toUriString();
        }
        for (var warc : pager.items) {
            String url = baseUrl + "/data/warcs/" + warc.getId();
            String textUrl = baseUrl + "/data/text/" + warc.getId();
            list.add(new WarcData(url, textUrl, warc));
        }
        return list;
    }

    @GetMapping("/data/warcs/{warcId}")
    public void getWarc(@PathVariable long warcId,
                        @RequestHeader(value = "Range", required = false) String rangeHeader,
                        HttpServletRequest request,
                        HttpServletResponse response) throws AccessDeniedException, MissingCredentialsException {
        enforceAgwaCredentials(request);
        var warc = wa.warcs.get(warcId);
        var crawl = wa.crawls.get(warc.getCrawlId());
        enforceAgwaCrawl(crawl);
        warcsController.serveWarc(warc, rangeHeader, request, response);
    }

    @GetMapping(value = "/data/text/{warcId}", produces = "application/json")
    public void getText(@PathVariable long warcId,
                                HttpServletRequest request,
                                HttpServletResponse response) throws AccessDeniedException, MissingCredentialsException, IOException {
        enforceAgwaCredentials(request);
        var warc = wa.warcs.get(warcId);
        var crawl = wa.crawls.get(warc.getCrawlId());
        enforceAgwaCrawl(crawl);
        warcsController.serveText(request, response, warc, crawl);
    }

    @GetMapping(value = "/data/solr", produces = "application/json")
    public void search(@RequestParam MultiValueMap<String, String> params,
                       HttpServletRequest request,
                        HttpServletResponse response) throws AccessDeniedException, MissingCredentialsException, IOException {
        enforceAgwaCredentials(request);
        var solrParams = new LinkedMultiValueMap<String, String>();
        solrParams.set("fq", "+auGov:true -discoverable:false");
        solrParams.addAll(params);
        var timeAllowed = Optional.ofNullable(solrParams.getFirst("timeAllowed")).map(Long::parseLong).orElse(0L);
        if (timeAllowed > 60000L) timeAllowed = 60000L;
        solrParams.set("timeAllowed", String.valueOf(timeAllowed));
        if (!solrParams.containsKey("wt")) {
            solrParams.add("wt", "json");
        }
        if (solrQueriesInFlight.incrementAndGet() > 5) {
            solrQueriesInFlight.decrementAndGet();
            response.setStatus(429);
            response.getWriter().write("Too many queries in flight");
            return;
        }
        try {
            var url = URI.create("http://wa-solr-prd-1.nla.gov.au:20001/solr/webarchive/select?" +
                    UriUtils.encodeQueryParams(solrParams)).toURL();
            var conn = (HttpURLConnection) url.openConnection();
            response.setStatus(conn.getResponseCode());
            response.setContentType(conn.getContentType());

            var err = conn.getErrorStream();
            if (err != null) {
                try (err; var out = response.getOutputStream()) {
                    err.transferTo(out);
                }
                return;
            }

            try (var in = conn.getInputStream();
                 var out = response.getOutputStream()) {
                in.transferTo(out);
            }
        } finally {
            solrQueriesInFlight.decrementAndGet();
        }
    }

    record WarcData(
            String url,
            String textUrl,
            long size,
            String sha256,
            long records,
            long recordBytes,
            Instant startTime,
            Instant endTime,
            String filename) {
        public WarcData(String url, String textUrl, Warc warc) {
            this(url, textUrl,
                    warc.getSize(),
                    warc.getSha256(),
                    warc.getRecords(),
                    warc.getRecordBytes(),
                    warc.getStartTime() == null ? null : warc.getStartTime().toInstant(),
                    warc.getEndTime() == null ? null : warc.getEndTime().toInstant(),
                    warc.getFilename());
        }
    }
}
