package bamboo.crawl;

import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface WarcRepository extends CrudRepository<Warc, Long> {
    Optional<Warc> findByFilename(@Param("filename") String filename);
}
