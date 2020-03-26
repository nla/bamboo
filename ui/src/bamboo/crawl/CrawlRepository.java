package bamboo.crawl;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CrawlRepository extends PagingAndSortingRepository<Crawl, Long> {
    Optional<Crawl> findByWebrecorderCollectionId(@Param("webrecorderCollectionId") String webrecorderCollectionId);
}
