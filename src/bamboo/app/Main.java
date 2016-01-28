package bamboo.app;

import droute.Handler;
import droute.ShotgunHandler;
import droute.nanohttpd.NanoServer;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;

public class Main {

    public static void usage() {
        System.out.println("Usage: bamboo <subcommand>");
        System.out.println("Bamboo admin tools");
        System.out.println("\nSub-commands:");
        System.out.println("  cdx-indexer                      - Run the CDX indexer");
        System.out.println("  import <jobName> <crawlSeriesId> - Import a crawl from Heritrix");
        System.out.println("  insert-warc <crawl-id> <paths>   - Register WARCs with a crawl");
        System.out.println("  recalc-crawl-times               - Fill approx crawl times based on warc filenames (migration hack)");
        System.out.println("  refresh-warc-stats               - Refresh warc stats tables");
        System.out.println("  refresh-warc-stats-fs            - Refresh warc stats tables based on disk");
        System.out.println("  server                           - Run web server");
        System.out.println("  watch-importer <crawl-id> <path> - Monitor path for new warcs, incrementally index them and then import them to crawl-id");
        System.out.println("  import-pandas-instance  <series-id> <instance-id>");
        System.exit(1);
    }

    public static void main(String args[]) throws Exception {
        if (args.length == 0)
            usage();
        if (args[0].equals("server")) {
            server(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
            /*
        Bamboo bamboo = new Bamboo();
        switch (args[0]) {
            case "import":
                bamboo.crawls.importHeritrixCrawl(args[1], Long.parseLong(args[2]));
                break;
            case "insert-warc":
                for (int i = 2; i < args.length; i++) {
                    bamboo.insertWarc(Long.parseLong(args[1]), args[i]);
                }
                break;
            case "cdx-indexer":
                bamboo.runCdxIndexer();
                break;
            case "solr-indexer":
                bamboo.runSolrIndexer();
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
            case "import-pandas-instance":
                //new PandasImport(bamboo).importInstance(Long.parseLong(args[1]), Long.parseLong(args[2]));
                break;
            default:
                usage();
        }
                */
    }


    private static void serverUsage() {
        System.err.println("Usage: java " + Bamboo.class.getName() + " [-b bindaddr] [-p port] [-i]");
        System.err.println("");
        System.err.println("  -b bindaddr   Bind to a particular IP address");
        System.err.println("  -i            Inherit the server socket via STDIN (for use with systemd, inetd etc)");
        System.err.println("  -p port       Local port to listen on");
    }

    public static void server(String[] args) throws IOException {
        int port = 8080;
        String host = null;
        boolean inheritSocket = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "-b":
                    host = args[++i];
                    break;
                case "-i":
                    inheritSocket = true;
                    break;
                default:
                    serverUsage();
                    System.exit(1);
            }
        }
        Handler handler = new ShotgunHandler("bamboo.app.Webapp");
        if (inheritSocket) {
            Channel channel = System.inheritedChannel();
            if (channel != null && channel instanceof ServerSocketChannel) {
                new NanoServer(handler, ((ServerSocketChannel) channel).socket()).startAndJoin();
                System.exit(0);
            }
            System.err.println("When -i is given STDIN must be a ServerSocketChannel, but got " + channel);
            System.exit(1);
        }
        if (host != null) {
            new NanoServer(handler, host, port).startAndJoin();
        } else {
            new NanoServer(handler, port).startAndJoin();
        }
    }

}
