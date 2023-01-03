package bamboo.crawl;

import bamboo.app.Bamboo;
import bamboo.task.WarcToIndex;
import bamboo.util.Markdown;
import bamboo.util.Pager;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Controller
public class CollectionsController {

    final Bamboo bamboo;

    private static final Gson gson;

    static {
        GsonBuilder builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String indent = System.getProperty("disableJsonIndent");
        if (indent != null && "true".equals(indent)) {
            gson = builder.create();
        } else {
            gson = builder.setPrettyPrinting().create();
        }
    }

    public CollectionsController(Bamboo bamboo) {
        this.bamboo = bamboo;
    }

    @GetMapping("/collections")
    String index(@RequestParam(value = "page", defaultValue = "1") long page, Model model) {
        Pager<Collection> pager = bamboo.collections.paginate(page);
        model.addAttribute("collections", pager.items);
        model.addAttribute("collectionsPager", pager);
        return "collections/index";
    }

    @GetMapping("/collections/new")
    String newForm() {
        return "collections/new";
    }

    @PostMapping("/collections/new")
    String create(Collection collection) {
        long collectionId = bamboo.collections.create(collection);
        return "redirect:/collections/" + collectionId;
    }

    @GetMapping("/collections/{id}")
    String show(@PathVariable("id") long id, Model model, HttpServletRequest request) {
        Collection collection = bamboo.collections.get(id);
        model.addAttribute("collection", collection);
        model.addAttribute("descriptionHtml", Markdown.render(collection.getDescription(), request.getRequestURI()));
        return "collections/show";
    }

    @GetMapping("/collections/{id}/edit")
    String edit(@PathVariable("id") long id, Model model, HttpServletRequest request) {
        Collection collection = bamboo.collections.get(id);
        model.addAttribute("collection", collection);
        return "collections/edit";
    }

    @PostMapping("/collections/{id}/edit")
    String update(@PathVariable("id") long collectionId, Collection collection) {
        bamboo.collections.update(collectionId, collection);
        return "redirect:/collections/" + collectionId;
    }

    @GetMapping("/collections/{id}/warcs/json")
    void warcs(@PathVariable("id") long id,
               @RequestParam(value = "start", defaultValue = "0") long start,
               @RequestParam(value = "rows", defaultValue = "1000") long rows,
               HttpServletResponse response) throws IOException {
        List<Warc> warcs = bamboo.warcs.findByCollectionId(id, start, rows);

        response.setStatus(200);
        response.setContentType("application/json");

        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8))) {
            writer.beginArray();
            for (Warc warc : warcs) {
                gson.toJson(new BambooWarcToIndex(warc), BambooWarcToIndex.class, writer);
            }
            writer.endArray();
            writer.flush();
        }
    }

    @GetMapping("/collections/{id}/warcs/sync")
    void sync(@PathVariable("id") long collectionId,
              @RequestParam(value = "after", required = false) String afterParam,
              @RequestParam(value = "limit", defaultValue = "100") int limit,
              HttpServletResponse response) throws IOException {
        WarcResumptionToken after = afterParam == null ? WarcResumptionToken.MIN_VALUE : WarcResumptionToken.parse(afterParam);
        List<WarcResumptionToken> results = bamboo.warcs.resumptionByCollectionIdAndStateId(collectionId, 2, after, limit);

        response.setStatus(200);
        response.setContentType("application/json");

        try (JsonWriter writer = gson.newJsonWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8))) {
            writer.beginArray();
            for (WarcResumptionToken token : results) {
                writer.beginObject();
                writer.name("id").value(token.id);
                writer.name("resumptionToken").value(token.toString());
                writer.name("urlCount").value(token.urlCount);
                writer.endObject();
            }
            writer.endArray();
            writer.flush();
        }
    }

    class BambooWarcToIndex extends WarcToIndex {
        public BambooWarcToIndex(Warc warc) {
            super(warc.getId(), warc.getRecords());
        }
    }
}
