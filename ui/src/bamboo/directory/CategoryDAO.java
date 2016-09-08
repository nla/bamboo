package bamboo.directory;

import bamboo.crawl.Collection;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;

import java.util.List;

public interface CategoryDAO {

    @SqlUpdate("INSERT INTO dir_category (parent_id, name, description) VALUES (:parentId, :name, :description)")
    @GetGeneratedKeys
    long insertCategory(@BindBean Category category);

    @SqlQuery("SELECT id, parent_id parentId, name, description FROM dir_category WHERE parent_id = :parentId")
    @MapResultAsBean
    List<Category> listCategories(@Bind("parentId") Long parentId);

    @SqlQuery("SELECT id, parent_id parentId, name, description FROM dir_category WHERE id = :id")
    @MapResultAsBean
    Category findCategory(@Bind("id") long id);

    @SqlUpdate("UPDATE dir_category SET name = :category.name, description = :category.description WHERE id = :id")
    int update(@Bind("id") long categoryId, @BindBean("category") Category category);

}
