package bamboo.task;

import bamboo.app.Bamboo;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.io.IOException;

@Controller
public class JobsController {
    final Bamboo bamboo;

    public JobsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    @PostMapping("/jobs/{job}/delete")
    String delete(@PathVariable("job") String jobName) throws IOException {
        HeritrixJob job = HeritrixJob.byName(bamboo.config.getHeritrixJobs(), jobName);
        FileUtils.deleteDirectory(job.dir().toFile());
        return "redirect:/jobs";
    }

    @GetMapping("/jobs")
    String index(Model model) {
        model.addAttribute("heritrixUrl", bamboo.config.getHeritrixUrl());
        model.addAttribute("jobs", HeritrixJob.list(bamboo.config.getHeritrixJobs()));
        return "jobs/index";
    }
}