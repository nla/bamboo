package bamboo;

import org.h2.jdbcx.*;
import org.skife.jdbi.v2.*;

import pandas.*;

public interface PandasDB extends AutoCloseable {
	
	public static PandasDB open() {
		PandasConfig config = new PandasConfig();
		config.migrateDb();
		JdbcConnectionPool pool = JdbcConnectionPool.create(config.getDbUrl(), config.getDbUser(), config.getDbPassword());
		DBI dbi = new DBI(pool);
		return dbi.open(PandasDB.class);
	}

	public static void addWarc(long blobId, String filename, String launch);
}
