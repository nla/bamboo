package bamboo.directory;

import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;

import java.util.List;

public interface DirectoryDAO {

    @SqlUpdate("INSERT INTO dir_category (parent_id, name, description) VALUES (:parentId, :name, :description)")
    @GetGeneratedKeys
    long insertCategory(@BindBean Category category);

    @SqlQuery("SELECT id, parent_id parentId, name, description FROM dir_category")
    @MapResultAsBean
    List<Category> listCategories();

    @SqlQuery("SELECT id, parent_id parentId, name, description FROM dir_category WHERE id = :id")
    @MapResultAsBean
    Category findCategory(@Bind("id") long id);
}
