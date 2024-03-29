package bamboo.crawl;

import bamboo.User;
import bamboo.app.Bamboo;
import bamboo.core.Permission;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;

import static java.util.Collections.emptyList;

@Controller
public class SeriesController {
    private final Bamboo wa;
    private final SeriesRepository seriesRepository;

    public SeriesController(Bamboo wa, SeriesRepository seriesRepository) {
        this.wa = wa;
        this.seriesRepository = seriesRepository;
    }

    @GetMapping("/series")
    @PreAuthorize("hasAnyAuthority('PERM_SERIES_VIEW_AGENCY', 'PERM_SERIES_VIEW_ALL')")
    String index(@RequestParam(value = "page", defaultValue = "1") int page, Model model,
                 @AuthenticationPrincipal User user) {
        Page<Series> pager;
        if (user == null || user.hasAuthority(Permission.SERIES_VIEW_ALL)) {
            pager = seriesRepository.findAll(PageRequest.of(page - 1, 100));
        } else if (user.hasAuthority(Permission.SERIES_VIEW_AGENCY)) {
            if (user.getAgencyId() == null) {
                throw new IllegalStateException("user has no agencyId");
            }
            model.addAttribute("agency", wa.agencies.getOrNull(user.getAgencyId()));
            pager = seriesRepository.findByAgencyId(user.getAgencyId(), PageRequest.of(page - 1, 100));
        } else {
            throw new IllegalStateException();
        }
        model.addAttribute("seriesList", pager.toList());
        model.addAttribute("seriesPager", pager);
        return "series/index";
    }

    @GetMapping("/series/new")
    @PreAuthorize("hasAnyAuthority('PERM_SERIES_EDIT_AGENCY', 'PERM_SERIES_EDIT_ALL')")
    String newForm() {
        return "series/new";
    }

    @PostMapping("/series/new")
    @PreAuthorize("hasAnyAuthority('PERM_SERIES_EDIT_AGENCY', 'PERM_SERIES_EDIT_ALL')")
    String createSeries(Series series, @AuthenticationPrincipal User user) {
        if (user.getAgencyId() != null) {
            series.setAgencyId(user.getAgencyId());
        }
        long seriesId = wa.serieses.create(series);
        return "redirect:/series/" + seriesId;
    }

    @GetMapping("/series/{id}")
    @PreAuthorize("hasPermission(#id, 'Series', 'view')")
    String show(@PathVariable("id") long id,
                @RequestParam(value = "page", defaultValue = "1") long page,
                Model model, HttpServletRequest request) {
        Series series = wa.serieses.get(id);
        Agency agency = series.getAgencyId() == null ? null : wa.agencies.get(series.getAgencyId());
        Pager<Crawl> crawlPager = wa.crawls.paginateWithSeriesId(page, id);
        model.addAttribute("series", series);
        model.addAttribute("agency", agency);
        model.addAttribute("descriptionHtml", Markdown.render(series.getDescription(), request.getRequestURI()));
        model.addAttribute("crawlList", crawlPager.items);
        model.addAttribute("crawlPager", crawlPager);
        model.addAttribute("collections", wa.collections.listWhereSeriesId(id));
        return "series/show";
    }

    @GetMapping("/series/{id}/edit")
    @PreAuthorize("hasPermission(#id, 'Series', 'edit')")
    String edit(@PathVariable("id") long id, Model model) {
        Series series = wa.serieses.get(id);
        model.addAttribute("series", series);
        model.addAttribute("collections", wa.collections.listWhereSeriesId(id));
        model.addAttribute("allCollections", wa.collections.listAll());
        return "series/edit";
    }

    @PostMapping("/series/{id}/edit")
    @PreAuthorize("hasPermission(#seriesId, 'Series', 'edit')")
    String update(@PathVariable("id") long seriesId,
                  Series series,
                  @RequestParam(value = "collection.id", required = false) List<Long> collectionIds) {
        if (collectionIds == null) collectionIds = emptyList();

        wa.serieses.update(seriesId, series, collectionIds);
        return "redirect:/series/" + seriesId;
    }
}
