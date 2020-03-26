package bamboo.crawl;

import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface SeriesRepository extends CrudRepository<Series, Long> {
    List<Series> findByAgencyIdAndName(Integer agencyId, String name);
}
