package bamboo.core;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PandasImport {
    final Bamboo bamboo;
    final Path pandasWarcDir = Paths.get("/derivative/data/nla.arc/warc");

    public PandasImport(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    /*
    void importInstance(long seriesId, long instanceId) throws IOException {
        try (Db db = bamboo.dbPool.take();
             PandasDb pandasDb = bamboo.pandasDbPool.take()) {
            PandasDb.InstanceSummary instance = pandasDb.fetchInstanceSummary(instanceId);

            db.createPandasCrawl(instance.titleName, seriesId, instanceId, listWarcs(instance));

        }
    }

    List<PandasWarc> listWarcs(PandasDb.InstanceSummary instance) throws IOException {
        List<PandasWarc> warcs = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolveTitlePath(instance.pi))) {
            for (Path entry : stream) {
                String filename = entry.getFileName().toString();
                if (filename.startsWith("nla.arc-" + instance.pi + "-" + instance.date + "-") &&
                        filename.endsWith(".warc.gz")) {
                    System.out.println(entry);
                    warcs.add(new PandasWarc(entry));
                }
            }
        }
        return warcs;
    }

    Path resolveTitlePath(long pi) {
        return pandasWarcDir.resolve(String.format("%03d", pi / 1000)).resolve(Long.toString(pi));
    }
    */

}
