package bamboo.core;

import bamboo.crawl.*;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.*;
import org.skife.jdbi.v2.sqlobject.mixins.Transactional;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public abstract class Db implements AutoCloseable, Transactional {

	public abstract  void close();






}
