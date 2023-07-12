package bamboo.api;

import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.util.List;

public interface ApiDAO {
    @SqlQuery("""
            SELECT
                warc.id as warcId,
                warc.filename,
                crawl.id as crawlId,
                crawl.name as crawlName,
                crawl.pandas_instance_id as pandasInstanceId
            FROM warc
            LEFT JOIN crawl ON crawl.id = warc.crawl_id
            WHERE filename in (<filenames>)
            """)
    @RegisterConstructorMapper(CrawlsByFilename.class)
    List<CrawlsByFilename> crawlsByWarcFilename(@BindList("filenames") List<String> filenames);

    record CrawlsByFilename(long warcId, String filename, long crawlId, String crawlName, Long pandasInstanceId) {
    }
}
