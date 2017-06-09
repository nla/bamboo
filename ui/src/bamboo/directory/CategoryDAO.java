package bamboo.directory;

import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.helpers.MapResultAsBean;

import java.util.List;

public interface CategoryDAO {

    String SELECT_DEFAULT_FIELDS = "SELECT id, parent_id parentId, name, description, pandas_subject_id pandasSubjectId FROM dir_category";

    @SqlUpdate("INSERT INTO dir_category (parent_id, name, description) VALUES (:parentId, :name, :description)")
    @GetGeneratedKeys
    long insertCategory(@BindBean Category category);

    @SqlQuery(SELECT_DEFAULT_FIELDS + " WHERE parent_id = :parentId")
    @MapResultAsBean
    List<Category> listCategories(@Bind("parentId") Long parentId);

    @SqlQuery(SELECT_DEFAULT_FIELDS + " WHERE id = :id")
    @MapResultAsBean
    Category findCategory(@Bind("id") long id);

    @SqlQuery(SELECT_DEFAULT_FIELDS + " WHERE pandas_subject_id = :subjectId")
    @MapResultAsBean
    Category findCategoryByPandasSubjectId(@Bind("subjectId") long subjectId);

    @SqlQuery(SELECT_DEFAULT_FIELDS + " WHERE legacy_type_id = :legacyTypeId AND legacy_id = :legacyId")
    @MapResultAsBean
    Category findCategoryByLegacyId(@Bind("legacyTypeId") int legacyTypeId, @Bind("legacyId") long legacyId);

    @SqlUpdate("UPDATE dir_category SET name = :category.name, description = :category.description WHERE id = :id")
    int update(@Bind("id") long categoryId, @BindBean("category") Category category);

    @SqlUpdate("INSERT INTO dir_symlink (parent_id, target_id) SELECT :parentId, :targetId FROM DUAL WHERE NOT EXISTS (SELECT * FROM dir_symlink WHERE parent_id = :parentId AND target_id = :targetId)")
    void insertSymlinkIfNotExists(@Bind("parentId") long parentId, @Bind("targetId") long targetId);
}
