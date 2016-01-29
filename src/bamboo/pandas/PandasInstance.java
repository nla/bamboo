package bamboo.pandas;

import bamboo.core.Config;
import bamboo.crawl.Crawl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class PandasInstance {
    private final Config config;
    public final long id;
    public final long pi;
    public final String date;
    public final String titleName;

    PandasInstance(Config config, ResultSet rs) throws SQLException {
        this.config = config;
        id = rs.getLong("instance_id");
        pi = rs.getLong("pi");
        date = rs.getString("dt");
        titleName = rs.getString("name");
    }

    List<Path> warcFiles() throws IOException {
        return Files.list(titlePath())
                .filter(this::isWarcFile)
                .collect(Collectors.toList());
    }

    private boolean isWarcFile(Path path) {
        String filename = path.getFileName().toString();
        return filename.startsWith("nla.arc-" + pi + "-" + date + "-") && filename.endsWith(".warc.gz");
    }

    private Path titlePath() {
        return config.getPandasWarcDir().resolve(String.format("%03d", pi / 1000)).resolve(Long.toString(pi));
    }

    Crawl toCrawl() {
        Crawl crawl = new Crawl();
        crawl.setPandasInstanceId(id);
        crawl.setName(titleName);
        return crawl;
    }
}
