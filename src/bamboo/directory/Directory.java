package bamboo.directory;

import java.util.List;

public class Directory {
    private final DirectoryDAO dao;

    public Directory(DirectoryDAO dao) {
        this.dao = dao;
    }

    public long createCategory(Category category) {
        return dao.insertCategory(category);
    }

    public List<Category> traverse() {
        return dao.listCategories();
    }

    public Category getCategory(long id) {
        return dao.findCategory(id);
    }
}
