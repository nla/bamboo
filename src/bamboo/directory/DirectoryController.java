package bamboo.directory;

import bamboo.app.Bamboo;
import bamboo.util.Parsing;
import droute.*;

import static droute.Response.render;
import static droute.Response.seeOther;
import static droute.Route.GET;
import static droute.Route.POST;

public class DirectoryController {
    final Directory directory;
    public final Handler routes = Route.routes(
            GET("/directory", this::index),
            GET("/directory/category/new", this::categoryNew),
            POST("/directory/category/create", this::categoryCreate),
            GET("/directory/category/:id/edit", this::categoryEdit, "id", "[0-9]+")
    );

    private Response categoryCreate(Request request) {
        long id = directory.createCategory(parseCategoryForm(request));
        return seeOther(request.contextUri().resolve("directory/category/" + id).toString());
    }

    private Category parseCategoryForm(Request request) {
        Category category = new Category();
        category.setParentId(Parsing.parseLongOrDefault(request.formParam("parentId"), null));
        category.setName(request.formParam("name"));
        category.setDescription(request.formParam("description"));
        return category;
    }

    private Response categoryNew(Request request) {
        return render("bamboo/directory/views/category/new.ftl",
                "csrfToken", Csrf.token(request));
    }

    private Response categoryEdit(Request request) {
        return render("bamboo/directory/views/category/edit.ftl",
                "csrfToken", Csrf.token(request),
                "category", directory.getCategory(Long.parseLong(request.urlParam("id"))));
    }

    private Response index(Request request) {
        return render("bamboo/directory/views/index.ftl",
                "categories", directory.traverse());
    }

    public DirectoryController(Bamboo bamboo) {
        this.directory = bamboo.directory;
    }
}
