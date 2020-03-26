package bamboo.crawl;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;

public interface SeriesRepository extends PagingAndSortingRepository<Series, Long> {
    // used by bamboo-webrecorder-sync
    List<Series> findByAgencyIdAndName(Integer agencyId, String name);

    Page<Series> findByAgencyId(Integer agencyId, Pageable pageable);
}
