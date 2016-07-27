package bamboo.directory;

import bamboo.core.NotFoundException;

import java.util.List;

public class Categories {
    private final CategoryDAO dao;

    public Categories(CategoryDAO dao) {
        this.dao = dao;
    }

    public long create(Category category) {
        return dao.insertCategory(category);
    }

    public List<Category> traverse() {
        return dao.listCategories(null);
    }

    public Category get(long id) {
        return NotFoundException.check(getOrNull(id), "category", id);
    }

    public Category getOrNull(long id) {
        return dao.findCategory(id);
    }

    public void update(long categoryId, Category category) {
        int rows = dao.update(categoryId, category);
        if (rows == 0) {
            throw new NotFoundException("category", categoryId);
        }
    }

    public List<Category> listSubcategories(long id) {
        return dao.listCategories(id);
    }
}
