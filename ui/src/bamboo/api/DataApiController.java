package bamboo.api;

import bamboo.app.Bamboo;
import bamboo.core.NotFoundException;
import bamboo.crawl.Crawl;
import bamboo.crawl.Warc;
import bamboo.crawl.WarcsController;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.PathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Instant;
import java.util.Base64;
import java.util.Set;

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

    @GetMapping("/data/warcsByCrawl/{crawlId}")
    public void listWarcsByCrawl(@PathVariable long crawlId,
                                 HttpServletRequest request,
                                 UriComponentsBuilder uriBuilder) throws AccessDeniedException, MissingCredentialsException {
        enforceAgwaCredentials(request);
        var crawl = wa.crawls.get(crawlId);
        enforceAgwaCrawl(crawl);
        var pager = wa.warcs.paginateWithCrawlId(0, crawlId);
        for (var warc : pager.items) {
            String url = uriBuilder.path("/data/warcs").pathSegment(Long.toString(warc.getId())).toUriString();
            String textUrl = uriBuilder.path("/data/text").pathSegment(Long.toString(warc.getId())).toUriString();
            new WarcData(url, textUrl, warc);
        }
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
    @ResponseBody
    public PathResource getText(@PathVariable long warcId,
                                HttpServletRequest request,
                                HttpServletResponse response) throws AccessDeniedException, MissingCredentialsException {
        enforceAgwaCredentials(request);
        if (warcsController.textCache == null) {
            throw new NotFoundException("Text cache is disabled");
        }
        var warc = wa.warcs.get(warcId);
        var crawl = wa.crawls.get(warc.getCrawlId());
        enforceAgwaCrawl(crawl);
        var textPath = warcsController.textCache.find(warcId);
        if (textPath == null) {
            throw new NotFoundException("No text for warc " + warcId);
        }
        response.setHeader("Content-Encoding", "gzip");
        return new PathResource(textPath);
    }

    record WarcData(
            String url,
            String textUrl,
            long size,
            String sha256,
            long records,
            long recordBytes,
            Instant startTime,
            Instant endTime) {
        public WarcData(String url, String textUrl, Warc warc) {
            this(url, textUrl,
                    warc.getSize(),
                    warc.getSha256(),
                    warc.getRecords(),
                    warc.getRecordBytes(),
                    warc.getStartTime() == null ? null : warc.getStartTime().toInstant(),
                    warc.getEndTime() == null ? null : warc.getEndTime().toInstant());
        }
    }
}
