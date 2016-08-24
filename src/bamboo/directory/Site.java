package bamboo.directory;

import java.sql.Date;

public class Site {
    private Long id;
    private Long categoryId;
    private String name;
    private String url;
    private Date date;
    private Long pandasTitleId;
    private Long displayOrder;
    private Integer displayLevel;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Long getPandasTitleId() {
        return pandasTitleId;
    }

    public void setPandasTitleId(Long pandasTitleId) {
        this.pandasTitleId = pandasTitleId;
    }

    public Long getDisplayOrder() {
        return displayOrder;
    }

    public void setDisplayOrder(Long displayOrder) {
        this.displayOrder = displayOrder;
    }

    public Integer getDisplayLevel() {
        return displayLevel;
    }

    public void setDisplayLevel(Integer displayLevel) {
        this.displayLevel = displayLevel;
    }
}
