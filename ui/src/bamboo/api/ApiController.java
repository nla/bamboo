package bamboo.api;

import bamboo.app.Bamboo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
public class ApiController {
    private final Bamboo wa;

    public ApiController(Bamboo wa) {
        this.wa = wa;
    }

    // Used by pandas-admin for the pages feature
    @GetMapping("/api/v1/CrawlsByFilename")
    @ResponseBody
    public List<ApiDAO.CrawlsByFilename> crawlsByFilename(@RequestParam("filename") List<String> filenames) {
        return wa.dao.api().crawlsByFilename(filenames);
    }
}
