package bamboo.app;

import bamboo.core.Config;
import bamboo.crawl.Scrub;
import bamboo.crawl.Warc;
import bamboo.task.CdxCache;
import bamboo.task.TextCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CLI {
    public static void usage() {
        System.out.println("Usage: bamboo <subcommand>");
        System.out.println("Bamboo admin tools");
        System.out.println("\nSub-commands:");
//        System.out.println("  cdx-indexer                      - Run the CDX indexer");
        System.out.println("  import <jobName> <crawlSeriesId> - Import a crawl from Heritrix");
        System.out.println("  insert-warc <crawl-id> <paths>   - Register WARCs with a crawl");
//        System.out.println("  recalc-crawl-times               - Fill approx crawl times based on warc filenames (migration hack)");
        System.out.println("  recalculate-warc-stats           - Refresh warc stats tables");
//        System.out.println("  refresh-warc-stats-fs            - Refresh warc stats tables based on disk");
        System.out.println("  server                           - Run web server");
        System.out.println("  watch-importer <crawl-id> <path> - Monitor path for new warcs, incrementally index them and then import them to crawl-id");
        System.out.println("  import-pandas-instance  <series-id> <instance-id>");
        System.out.println("  import-pandas-all  <series-id>     - Import all pandas instances");
        System.out.println("  import-pandas-artifacts <crawl-id> - Import pandas artifacts for given crawl id");
        System.out.println("  import-pandas-artifacts-all        - Import artifacts for all pandas crawls");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            usage();
            return;
        }
        try (Bamboo bamboo = new Bamboo(new Config(System.getenv()), false)) {
            switch (args[0]) {
                case "import-pandas-all":
                    checkPandasIntegration(bamboo);
                    bamboo.pandas.importAllInstances(Long.parseLong(args[1]));
                    break;
                case "import-pandas":
                    checkPandasIntegration(bamboo);
                    bamboo.pandas.importAllInstances(Long.parseLong(args[1]), args[2]);
                    break;
                case "import-pandas-instance":
                    checkPandasIntegration(bamboo);
                    bamboo.pandas.importInstanceIfNotExists(Long.parseLong(args[2]), Long.parseLong(args[1]));
                    break;
                case "import":
                    bamboo.crawls.importHeritrixCrawl(args[1], Long.parseLong(args[2]));
                    break;
                case "insert-warc":
                    long crawlId = Long.parseLong(args[1]);
                    for (int i = 2; i < args.length; i++) {
                        Path path = Paths.get(args[i]);
                        if (!Files.exists(path)) {
                            System.err.println("File not found: " + path);
                            System.exit(1);
                        }
                        long size = Files.size(path);
                        String digest = Scrub.calculateDigest("SHA-256", path);
                        bamboo.warcs.create(crawlId, Warc.IMPORTED, path, path.getFileName().toString(), size, digest);
                    }
                    break;

                case "move-warc-to-blob-storage": {
                    for (int i = 1; i < args.length; i++) {
                        long warcId = Long.parseLong(args[i]);
                        System.out.print("warc " + warcId + " ");
                        System.out.flush();
                        String status = bamboo.warcs.moveToBlobStorage(warcId);
                        System.out.println(status);
                    }
                    break;
                }

                case "recalculate-warc-stats":
                    System.out.println("Recalculating WARC stats for crawls");
                    bamboo.crawls.recalculateWarcStats();
                    System.out.println("Recalculating WARC stats for serieses");
                    bamboo.serieses.recalculateWarcStats();
                    break;

                case "build-cdx-cache": {
                    long startId = -1;
                    long endId = Long.MAX_VALUE;
                    if (args.length > 2) {
                        startId = Long.parseLong(args[2]);
                        if (args.length > 3) {
                            endId = Long.parseLong(args[3]);
                        }
                    }
                    new CdxCache(Paths.get(args[1]), bamboo.warcs).populateAll(startId, endId);
                    break;
                }

                case "build-text-cache": {
                    long startId = -1;
                    long endId = Long.MAX_VALUE;
                    if (args.length > 2) {
                        startId = Long.parseLong(args[2]);
                        if (args.length > 3) {
                            endId = Long.parseLong(args[3]);
                        }
                    }
                    new TextCache(Paths.get(args[1]), bamboo.warcs, bamboo.textExtractor).populateAll(startId, endId);
                    break;
                }

                case "build-text-cache-series": {
                    new TextCache(Paths.get(args[1]), bamboo.warcs, bamboo.textExtractor).populateSeries(Long.parseLong(args[2]));
                    break;
                }

                case "import-pandas-artifacts":
                    bamboo.pandas.importInstanceArtifacts(Long.parseLong(args[1]));
                    break;

                case "import-pandas-artifacts-all":
                    bamboo.pandas.importAllInstanceArtifacts();
                    break;


            /* FIXME: restore these
            case "cdx-indexer":
                bamboo.runCdxIndexer();
                break;
            case "recalc-crawl-times":
                bamboo.recalcCrawlTimes();
                break;
            case "refresh-warc-stats":
                bamboo.refreshWarcStats();
                break;
            case "refresh-warc-stats-fs":
                bamboo.refreshWarcStatsFs();
                break;
            case "scrub":
                Scrub.scrub(bamboo);
                break;
            */
                default:
                    usage();
            }
        }
    }

    private static void checkPandasIntegration(Bamboo bamboo) {
        if (bamboo.pandas == null) {
            System.err.println("PANDAS integration not available, ensure PANDAS_DB_URL is set");
            System.exit(1);
        }
    }
}
