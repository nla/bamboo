package bamboo.api;

import bamboo.app.Bamboo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
public class ApiV2Controller {
    private final Bamboo wa;

    public ApiV2Controller(Bamboo wa) {
        this.wa = wa;
    }

    // Used by pandas-admin for the pages feature
    @GetMapping("/api/v2/CrawlsByWarcFilename")
    @ResponseBody
    public List<ApiDAO.CrawlsByFilename> crawlsByWarcFilename(@RequestParam("filename") List<String> filenames) {
        return wa.dao.api().crawlsByWarcFilename(filenames);
    }
}
