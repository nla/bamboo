package bamboo.directory;

public class Agency {
    private Long id;
    private String name;
    private byte[] logo;
    private String url;
    private Integer legacyTypeId;
    private Long legacyId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public byte[] getLogo() {
        return logo;
    }

    public void setLogo(byte[] logo) {
        this.logo = logo;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public void setLegacyId(int legacyTypeId, long legacyId) {
        setLegacyTypeId(legacyTypeId);
        setLegacyId(legacyId);
    }
}
