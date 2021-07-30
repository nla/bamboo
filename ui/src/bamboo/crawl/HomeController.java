package bamboo.crawl;

import bamboo.app.Bamboo;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;

@Controller
public class HomeController {
    private final Bamboo bamboo;

    public HomeController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("seriesList", bamboo.serieses.listAll());
        model.addAttribute("collections", bamboo.collections.listAll());
        Statistics warcStatistics = bamboo.warcs.getStatistics();
        Statistics artifactStatistics = bamboo.crawls.getArtifactStatistics();
        model.addAttribute("warcStatistics", warcStatistics);
        model.addAttribute("artifactStatistics", artifactStatistics);
        model.addAttribute("totalStatistics", warcStatistics.plus(artifactStatistics));
        return "index";
    }

    @GetMapping(value = "/healthcheck", produces = "text/plain")
    @ResponseBody
    public String healthcheck(HttpServletResponse response) {
        StringWriter out = new StringWriter();
        boolean ok = bamboo.healthcheck(new PrintWriter(out));
        response.setStatus(ok ? 200 : 500);
        return out.toString();
    }
}
