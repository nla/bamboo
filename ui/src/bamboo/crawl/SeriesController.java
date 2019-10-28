package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.springframework.http.HttpStatus.BAD_REQUEST;

@Controller
public class SeriesController {
    final Bamboo wa;

    public SeriesController(Bamboo wa) {
        this.wa = wa;
    }

    @GetMapping("/series")
    String index(@RequestParam(value = "page", defaultValue = "1") long page, Model model) {
        Pager<SeriesDAO.CrawlSeriesWithCount> pager = wa.serieses.paginate(page);
        model.addAttribute("seriesList", pager.items);
        model.addAttribute("seriesPager", pager);
        return "series/index";
    }

    @GetMapping("/series/new")
    String newForm() {
        return "series/new";
    }

    @PostMapping("/series/new")
    String createSeries(Series series) {
        long seriesId = wa.serieses.create(series);
        return "redirect:/series/" + seriesId;
    }

    @GetMapping("/series/{id}")
    String show(@PathVariable("id") long id,
                @RequestParam(value = "page", defaultValue = "1") long page,
                Model model, HttpServletRequest request) {
        Series series = wa.serieses.get(id);
        Pager<Crawl> crawlPager = wa.crawls.paginateWithSeriesId(page, id);
        model.addAttribute("series", series);
        model.addAttribute("descriptionHtml", Markdown.render(series.getDescription(), request.getRequestURI()));
        model.addAttribute("crawlList", crawlPager.items);
        model.addAttribute("crawlPager", crawlPager);
        model.addAttribute("collections", wa.collections.listWhereSeriesId(id));
        return "series/show";
    }

    @GetMapping("/series/{id}/edit")
    String edit(@PathVariable("id") long id, Model model) {
        Series series = wa.serieses.get(id);
        model.addAttribute("series", series);
        model.addAttribute("collections", wa.collections.listWhereSeriesId(id));
        model.addAttribute("allCollections", wa.collections.listAll());
        return "series/edit";
    }

    @PostMapping("/series/{id}/edit")
    String update(@PathVariable("id") long seriesId,
                  Series series,
                  @RequestParam(value = "collection.id", required = false) List<Long> collectionIds,
                  @RequestParam(value = "collection.urlFilters", required = false) List<String> collectionUrlFilters) {
        if (collectionIds == null) collectionIds = emptyList();
        if (collectionUrlFilters == null) collectionUrlFilters = emptyList();
        if (collectionIds.size() != collectionUrlFilters.size()) {
            throw new ResponseStatusException(BAD_REQUEST, "collection.id and collection.urlFilters mismatch");
        }

        wa.serieses.update(seriesId, series, collectionIds, collectionUrlFilters);
        return "redirect:/series/" + seriesId;
    }
}
