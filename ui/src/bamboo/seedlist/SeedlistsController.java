package bamboo.seedlist;

import bamboo.app.Bamboo;
import bamboo.pandas.Pandas;
import bamboo.pandas.PandasComparison;
import bamboo.util.Markdown;
import org.archive.url.SURT;
import org.archive.url.SURTTokenizer;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;

@Controller
public class SeedlistsController {
    private final Seedlists seedlists;
    private final Pandas pandas;

    public SeedlistsController(Bamboo bamboo) {
        seedlists = bamboo.seedlists;
        pandas = bamboo.pandas;
    }

    @GetMapping("/seedlists")
    String index(Model model) {
        model.addAttribute("seedlists", seedlists.listAll());
        return "seedlists/index";
    }

    @GetMapping("/seedlists/new")
    String newForm(Model model) {
        return "seedlists/new";
    }

    @GetMapping("/seedlists/{id1}/compare/{id2}/{sublist}/saveas")
    String saveComparison(@PathVariable("id1") long id1,
                          @PathVariable("id2") long id2,
                          @PathVariable("sublist") String sublist,
                          Model model) {
        Comparison comparison = new Comparison(seedlists, id1, id2);
        model.addAttribute("seeds", comparison.sublist(sublist));
        return "seedlists/new";
    }

    @PostMapping("/seedlists/create")
    String create(Form form) {
        long seedlistId = seedlists.create(form);
        return "redirect:/seedlists/" + seedlistId;
    }

    @PostMapping("/seedlists/{id}/edit")
    String update(@PathVariable("id") long seedlistId, Form form) {
        seedlists.update(seedlistId, form);
        return "redirect:/seedlists/" + seedlistId;
    }

    public static class Form implements Seedlists.Update {
        private String name;
        private String description;
        private String seeds;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public Collection<Seed> getSeeds() {
            Set<Seed> seeds = new HashSet<>();
            for (String url : this.seeds.split("\n")) {
                url = SURTTokenizer.addImpliedHttpIfNecessary(url.trim());
                seeds.add(new Seed(url));
            }
            return seeds;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void setSeeds(String seeds) {
            this.seeds = seeds;
        }
    }

    @GetMapping("/seedlists/{id}")
    String show(@PathVariable("id") long id, HttpServletRequest request, Model model) {
        Seedlist seedlist = seedlists.get(id);
        model.addAttribute("seedlist", seedlist);
        model.addAttribute("descriptionHtml", Markdown.render(seedlist.getDescription(), request.getRequestURI()));
        model.addAttribute("seeds", seedlists.listSeeds(id));
        return "seedlists/show";
    }

    @GetMapping("/seedlists/{id}/edit")
    String edit(@PathVariable("id") long id, Model model) {
        Seedlist seedlist = seedlists.get(id);
        model.addAttribute("seedlist", seedlist);
        model.addAttribute("seeds", seedlists.listSeeds(id));
        return "seedlists/edit";
    }

    @PostMapping("/seedlists/{id}/delete")
    String delete(@PathVariable("id") long seedlistId) {
        seedlists.delete(seedlistId);
        return "redirect:/seedlists";
    }

    @GetMapping("/seedlists/{id}/export/{format}")
    void export(@PathVariable("id") long seedlistId,
                @PathVariable("format") String format,
                HttpServletResponse response) throws IOException {
        export(seedlists.listSeeds(seedlistId), exportFormat(format), response);
    }

    @GetMapping("/seedlists/{id1}/compare/{id2}/{sublist}/export/{format}")
    private String exportComparison(@PathVariable("id1") long id1,
                                    @PathVariable("id2") long id2,
                                    @PathVariable("sublist") String sublistParam,
                                    @PathVariable("format") String formatName,
                                    HttpServletResponse response) throws IOException {
        List<Seed> sublist = new Comparison(seedlists, id1, id2).sublist(sublistParam);
        UnaryOperator<String> format = exportFormat(formatName);
        return export(sublist, format, response);
    }

    private String export(List<Seed> seeds, UnaryOperator<String> transformation, HttpServletResponse response) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8))) {
            for (Seed seed : seeds) {
                writer.write(transformation.apply(seed.getUrl()));
                writer.write("\n");
            }
            writer.flush();
            return "";
        }
    }

    private UnaryOperator<String> exportFormat(String formatName) {
        switch (formatName) {
            case "urls":
                return UnaryOperator.identity();
            case "surts":
                return SURT::toSURT;
            default:
                throw new IllegalArgumentException("unsupported export format");
        }
    }

    @GetMapping("/seedlists/{id}/compare")
    String compare(@PathVariable("id") long id, Model model) {
        Seedlist seedlist = seedlists.get(id);
        model.addAttribute("seedlist", seedlist);
        model.addAttribute("seedlists", seedlists.listAll());
        return "seedlists/compare";
    }

    @GetMapping("/seedlists/{id1}/compare/{id2}")
    String showComparison(@PathVariable("id1") long id1,
                          @PathVariable("id2") long id2,
                          Model model) {
        Comparison comparison = new Comparison(seedlists, id1, id2);
        model.addAttribute("seedlist1", comparison.seedlist1);
        model.addAttribute("seedlist2", comparison.seedlist2);
        model.addAttribute("onlyIn1", comparison.onlyIn1);
        model.addAttribute("onlyIn2", comparison.onlyIn2);
        model.addAttribute("common", comparison.common);
        return "seedlists/comparison";
    }

    @GetMapping("/seedlists/:id/compare/pandas")
    String compareWithPandas(@PathVariable("id") long seedlistId, Model model) {
        if (pandas == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "PANDAS integration not configured");
        }

        Seedlist seedlist = seedlists.get(seedlistId);
        PandasComparison comparison = pandas.compareSeedlist(seedlistId);

        model.addAttribute("seedlist", seedlist);
        model.addAttribute("matches", comparison.matches);
        model.addAttribute("unmatched", comparison.unmatched);
        return "seedlists/pandas";
    }
}
