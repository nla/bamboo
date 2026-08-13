package bamboo.virus;

import bamboo.core.LockManager;
import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import org.netpreserve.jwarc.MediaType;
import org.netpreserve.jwarc.WarcCaptureRecord;
import org.netpreserve.jwarc.WarcPayload;
import org.netpreserve.jwarc.WarcReader;
import org.netpreserve.jwarc.WarcRecord;
import org.netpreserve.jwarc.WarcResource;
import org.netpreserve.jwarc.WarcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VirusScanner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(VirusScanner.class);
    private static final String LOCK_NAME = "virus-scanner";

    private final VirusScanDAO dao;
    private final Warcs warcs;
    private final LockManager lockManager;
    private final ClamdClient clamd;
    private final Duration interval;
    private final int threads;
    private Instant clamdRetryAfter = Instant.EPOCH;

    public VirusScanner(VirusScanDAO dao, Warcs warcs, LockManager lockManager, ClamdClient clamd,
                        Duration interval, int threads) {
        this.dao = dao;
        this.warcs = warcs;
        this.lockManager = lockManager;
        this.clamd = clamd;
        this.interval = interval;
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        this.threads = threads;
    }

    @Override
    public void run() {
        if (clamdRetryAfter.isAfter(Instant.now())) return;
        if (!lockManager.takeLock(LOCK_NAME)) return;
        try {
            VirusScanRun run = dao.findRunningRun();
            if (run == null) {
                VirusScanRun latest = dao.findLatestFinishedRun();
                if (latest != null && latest.getFinishedAt().plus(interval).isAfter(Instant.now())) return;
                run = startRun();
            }
            scanNextBatch(run);
        } catch (IOException e) {
            clamdRetryAfter = Instant.now().plus(Duration.ofMinutes(1));
            log.warn("Unable to communicate with clamd", e);
        } finally {
            lockManager.releaseLock(LOCK_NAME);
        }
    }

    private VirusScanRun startRun() throws IOException {
        String version = abbreviate(clamd.version(), 255);
        long maxWarcId = dao.maxEligibleWarcId();
        long total = dao.countEligibleWarcs(maxWarcId);
        Timestamp now = now();
        long id = dao.createRun(maxWarcId, total, version, now);
        log.info("Started virus scan {} of {} WARCs using {}", id, total, version);
        return dao.findRun(id);
    }

    private void scanNextBatch(VirusScanRun run) throws IOException {
        List<Warc> candidates = warcs.streamForVirusScan(run.getLastWarcId(), run.getMaxWarcId(), threads);
        if (candidates.isEmpty()) {
            dao.finishRun(run.getId(), now());
            log.info("Completed virus scan {}", run.getId());
            return;
        }

        dao.setCurrentWarc(run.getId(), candidates.get(0).getId(), now());
        saveBatch(run.getId(), scanBatch(candidates));
    }

    private List<WarcOutcome> scanBatch(List<Warc> warcs) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(threads, warcs.size()));
        try {
            List<Future<WarcOutcome>> futures = new ArrayList<>();
            for (Warc warc : warcs) {
                futures.add(executor.submit(() -> new WarcOutcome(warc, scanWarc(warc))));
            }

            List<WarcOutcome> outcomes = new ArrayList<>();
            Throwable failure = null;
            for (Future<WarcOutcome> future : futures) {
                try {
                    outcomes.add(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while virus scanning", e);
                } catch (ExecutionException e) {
                    if (failure == null) failure = e.getCause();
                }
            }
            if (failure instanceof IOException ioException) throw ioException;
            if (failure instanceof RuntimeException runtimeException) throw runtimeException;
            if (failure != null) throw new RuntimeException(failure);
            return outcomes;
        } finally {
            executor.shutdownNow();
        }
    }

    private ScanOutcome scanWarc(Warc warc) throws IOException {
        List<VirusFinding> findings = new ArrayList<>();
        List<VirusScanProblem> problems = new ArrayList<>();
        long records = 0;
        long bytes = 0;

        try (ClamdClient.ScanSession session = clamd.openSession()) {
            try (WarcReader reader = new WarcReader(warcs.openStream(warc))) {
                WarcRecord record;
                while ((record = reader.next().orElse(null)) != null) {
                    long offset = reader.position();
                    if (!(record instanceof WarcResponse || record instanceof WarcResource) ||
                            !(record instanceof WarcCaptureRecord capture) || capture.payload().isEmpty()) {
                        continue;
                    }

                    records++;
                    WarcPayload payload = capture.payload().orElseThrow();
                    try {
                        ClamdClient.ScanResult result = session.scan(payload.body().stream());
                        bytes += result.bytes();
                        if (result.status() == ClamdClient.Status.FOUND) {
                            findings.add(finding(warc, offset, record, capture, payload, result.signature()));
                        } else if (result.status() == ClamdClient.Status.ERROR) {
                            problems.add(new VirusScanProblem(warc.getId(), offset, "clamd", abbreviate(result.reply(), 4096)));
                            break;
                        }
                    } catch (ClamdClient.InputReadException e) {
                        IOException cause = (IOException) e.getCause();
                        problems.add(new VirusScanProblem(warc.getId(), offset, "warc",
                                abbreviate(exceptionMessage(cause), 4096)));
                        log.warn("Skipping unreadable WARC payload {} at offset {}", warc.getPath(), offset, cause);
                        break;
                    } catch (IOException e) {
                        // Leave the cursor at this WARC and pause. Otherwise one daemon outage could create a problem
                        // row for every WARC in the archive.
                        throw new ClamdIOException(e);
                    }
                }
            } catch (ClamdIOException e) {
                throw (IOException) e.getCause();
            } catch (IOException e) {
                problems.add(new VirusScanProblem(warc.getId(), "warc", abbreviate(exceptionMessage(e), 4096)));
            }
        }

        return new ScanOutcome(findings, problems, records, bytes);
    }

    private static VirusFinding finding(Warc warc, long offset, WarcRecord record, WarcCaptureRecord capture,
                                        WarcPayload payload, String signature) {
        String mediaType;
        try {
            mediaType = payload.type().base().toString();
        } catch (IllegalArgumentException e) {
            mediaType = MediaType.OCTET_STREAM.toString();
        }
        String digest = payload.digest().map(value -> value.base32()).orElse(null);
        return new VirusFinding(warc.getId(), offset, abbreviate(record.id().toString(), 255),
                abbreviate(capture.target(), 4096), record.date(), abbreviate(mediaType, 255),
                abbreviate(digest, 128), abbreviate(signature, 128));
    }

    private void saveBatch(long runId, List<WarcOutcome> outcomes) {
        Timestamp now = now();
        dao.inTransaction(tx -> {
            long records = 0;
            long bytes = 0;
            long findings = 0;
            long errors = 0;
            for (WarcOutcome warcOutcome : outcomes) {
                Warc warc = warcOutcome.warc();
                ScanOutcome outcome = warcOutcome.outcome();
                saveOutcome(tx, runId, warc, outcome, now);
                records += outcome.records();
                bytes += outcome.bytes();
                findings += outcome.findings().size();
                errors += outcome.problems().size();
            }
            long lastWarcId = outcomes.get(outcomes.size() - 1).warc().getId();
            tx.advanceRun(runId, lastWarcId, outcomes.size(), records, bytes, findings, errors, now);
            return null;
        });
    }

    private static void saveOutcome(VirusScanDAO tx, long runId, Warc warc, ScanOutcome outcome, Timestamp now) {
        if (outcome.problems().isEmpty()) {
            tx.markFindingsNotDetected(warc.getId());
            tx.deleteProblems(warc.getId());
        }
        for (VirusFinding finding : outcome.findings()) {
            Timestamp captureTime = finding.captureTime() == null ? null : Timestamp.from(finding.captureTime());
            if (tx.updateFinding(runId, finding, captureTime, now) == 0) {
                tx.insertFinding(runId, finding, captureTime, now);
            }
        }
        for (VirusScanProblem problem : outcome.problems()) {
            if (tx.updateProblem(runId, problem, now) == 0) {
                tx.insertProblem(runId, problem, now);
            }
        }
    }

    private static Timestamp now() {
        return Timestamp.from(Instant.now());
    }

    private static String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) return value;
        return value.substring(0, maxLength);
    }

    static String exceptionMessage(Throwable error) {
        String message = error.getMessage();
        return error.getClass().getName() + (message == null || message.isBlank() ? "" : ": " + message);
    }

    private record ScanOutcome(List<VirusFinding> findings, List<VirusScanProblem> problems,
                               long records, long bytes) {
    }

    private record WarcOutcome(Warc warc, ScanOutcome outcome) {
    }

    private static class ClamdIOException extends IOException {
        ClamdIOException(IOException cause) {
            super(cause);
        }
    }
}
