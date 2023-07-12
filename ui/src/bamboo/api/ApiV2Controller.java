package bamboo.api;

import bamboo.app.Bamboo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Controller
public class ApiV2Controller {
    private final Bamboo wa;

    public ApiV2Controller(Bamboo wa) {
        this.wa = wa;
    }

    // Used by pandas-admin for the pages feature
    // supports both GET and POST in order to allow larger requests
    @RequestMapping(value = "/api/v2/CrawlsByWarcFilename", method = {GET, POST})
    @ResponseBody
    public List<ApiDAO.CrawlsByFilename> crawlsByWarcFilename(@RequestParam("filename") List<String> filenames) {
        return wa.dao.api().crawlsByWarcFilename(filenames);
    }
}
