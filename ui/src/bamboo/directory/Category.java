package bamboo.directory;

public class Category {
    private Long id;
    private Long parentId;
    private String name;
    private String description;
    private Integer legacyTypeId;
    private Long legacyId;

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Long getParentId() {
        return parentId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getLegacyTypeId() {
        return legacyTypeId;
    }

    public void setLegacyTypeId(Integer legacyTypeId) {
        this.legacyTypeId = legacyTypeId;
    }

    public Long getLegacyId() {
        return legacyId;
    }

    public void setLegacyId(Long legacyId) {
        this.legacyId = legacyId;
    }

    public void setLegacyId(Integer legacyTypeId, Long legacyId) {
        this.legacyTypeId = legacyTypeId;
        this.legacyId = legacyId;
    }

}
