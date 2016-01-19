package bamboo.seedlist;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Seedlist {
    private long id;
    private String name;
    private String description;
    private long totalSeeds;

	public Seedlist() {
	}

    public Seedlist(ResultSet rs) throws SQLException {
        setId(rs.getLong("id"));
        setName(rs.getString("name"));
        setDescription(rs.getString("description"));
        setTotalSeeds(rs.getLong("total_seeds"));
    }

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public long getTotalSeeds() {
		return totalSeeds;
	}

	public void setTotalSeeds(long totalSeeds) {
		this.totalSeeds = totalSeeds;
	}
}
