package bamboo.seedlist;

import com.google.common.net.InternetDomainName;
import org.apache.commons.lang.StringEscapeUtils;
import org.archive.url.SURT;
import org.archive.url.URLParser;

import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class Seed {
    private long id;
    private String url;
    private long seedlistId;

    public Seed(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setUrl(rs.getString("url"));
        setSeedlistId(rs.getLong("seedlist_id"));
    }

    public Seed(String url) {
        this.setUrl(url);
    }

    public String getSurt() {
        return SURT.toSURT(getUrl().replaceFirst("^[a-z]+://", "http://"));
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String topPrivateDomain() {
        try {
            return InternetDomainName.from(URLParser.parse(getUrl()).getHost()).topPrivateDomain().toString();
        } catch (URISyntaxException | IllegalArgumentException e) {
            return getUrl();
        }
    }

    public String highlighted() {
        String domain = StringEscapeUtils.escapeHtml(topPrivateDomain());
        String pattern = "(" + Pattern.quote(domain) + ")([:/]|$)";
        return "<span class='hlurl'>" + getUrl().replaceFirst(pattern, "<span class='domain'>$1</span>$2") + "</span>";
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSeedlistId() {
        return seedlistId;
    }

    public void setSeedlistId(long seedlistId) {
        this.seedlistId = seedlistId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Seed seed = (Seed) o;

        return url.equals(seed.url);

    }

    @Override
    public int hashCode() {
        return url.hashCode();
    }
}
