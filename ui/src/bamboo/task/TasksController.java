package bamboo.task;

import bamboo.app.Bamboo;
import bamboo.crawl.Warc;
import bamboo.crawl.Warcs;
import bamboo.util.Pager;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

@Controller
public class TasksController {
    private final Warcs warcs;
    private final TaskDAO taskDAO;

    public TasksController(Bamboo bamboo) {
        this.warcs = bamboo.warcs;
        this.taskDAO = bamboo.dao.tasks();
    }

    @GetMapping("/tasks")
    String index(Model model) {
        model.addAttribute("tasks", taskDAO.listTasks());
        return "view/tasks";
    }

    @PostMapping("/tasks/{id}/disable")
    String disable(@PathVariable("id") String id) {
        if (taskDAO.setEnabled(id, false) == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No such task");
        }
        return "redirect:/tasks";
    }

    @PostMapping("/tasks/{id}/enable")
    String enable(@PathVariable("id") String id) {
        if (taskDAO.setEnabled(id, true) == 0) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No such task");
        }
        return "redirect:/tasks";
    }

    @GetMapping("/tasks/CdxIndexer/queue")
    String cdxQueue(@RequestParam(value = "page", defaultValue = "1") long page, Model model) {
        Pager<Warc> pager = warcs.paginateWithState(page, Warc.IMPORTED);
        model.addAttribute("queueName", "CDX Indexing");
        model.addAttribute("warcs", pager.items);
        model.addAttribute("warcsPager", pager);
        return "tasks/warcs";
    }
}
