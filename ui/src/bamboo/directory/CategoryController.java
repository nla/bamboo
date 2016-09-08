package bamboo.directory;

import bamboo.app.Bamboo;
import bamboo.crawl.SeriesDAO;
import bamboo.util.Pager;
import bamboo.util.Parsing;
import droute.*;

import static droute.Response.render;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;

public class CategoryController {
    final Categories categories;
    public final Handler routes = Route.routes(
            GET("/directory", this::index),
            GET("/directory/category/new", this::newForm, "parentId", "[0-9]+"),
            POST("/directory/category/create", this::create),
            GET("/directory/category/:id", this::show, "id", "[0-9]+"),
            GET("/directory/category/:id/edit", this::edit, "id", "[0-9]+"),
            POST("/directory/category/:id/edit", this::update, "id", "[0-9]+")
    );

    private Response update(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        categories.update(id, parseForm(request));
        return seeOther(request.contextUri().resolve("directory/category/" + id).toString());
    }

    private Response show(Request request) {
        long id = Long.parseLong(request.urlParam("id"));
        return render("bamboo/directory/views/category/show.ftl",
                "csrfToken", Csrf.token(request),
                "category", categories.get(id),
                "subcategories", categories.listSubcategories(id));
    }

    private Response create(Request request) {
        long id = categories.create(parseForm(request));
        return seeOther(request.contextUri().resolve("directory/category/" + id).toString());
    }

    private Category parseForm(Request request) {
        Category category = new Category();
        category.setParentId(Parsing.parseLongOrDefault(request.formParam("parentId"), null));
        category.setName(request.formParam("name"));
        category.setDescription(request.formParam("description"));
        return category;
    }

    private Response newForm(Request request) {
        String parentId = request.queryParam("parentId");
        Category parent =  parentId != null ? categories.get(Long.parseLong(parentId)) : null;
        return render("bamboo/directory/views/category/new.ftl",
                "parent", parent,
                "csrfToken", Csrf.token(request));
    }

    private Response edit(Request request) {
        return render("bamboo/directory/views/category/edit.ftl",
                "csrfToken", Csrf.token(request),
                "category", categories.get(Long.parseLong(request.urlParam("id"))));
    }

    private Response index(Request request) {
        return render("bamboo/directory/views/index.ftl",
                "categories", categories.traverse());
    }

    public CategoryController(Bamboo bamboo) {
        this.categories = bamboo.categories;
    }
}
