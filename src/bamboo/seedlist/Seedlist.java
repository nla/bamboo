package bamboo.seedlist;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Seedlist {
    private long id;
    private String name;
    private String description;
    private long totalSeeds;

    Seedlist(ResultSet rs) throws SQLException {
		this.id = rs.getLong("id");
		this.name = rs.getString("name");
		this.description = rs.getString("description");
		this.totalSeeds = rs.getLong("total_seeds");
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public String getDescription() {
		return description;
	}

	public long getTotalSeeds() {
		return totalSeeds;
	}
}
