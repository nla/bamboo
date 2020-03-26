package bamboo.crawl;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface WarcRepository extends PagingAndSortingRepository<Warc, Long> {
    Optional<Warc> findByFilename(@Param("filename") String filename);
}
