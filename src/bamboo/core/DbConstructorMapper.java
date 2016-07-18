package bamboo.core;

import org.skife.jdbi.v2.ResultSetMapperFactory;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DbConstructorMapper implements ResultSetMapperFactory {
    @Override
    public boolean accepts(Class aClass, StatementContext statementContext) {
        try {
            aClass.getConstructor(ResultSet.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @Override
    public ResultSetMapper mapperFor(Class aClass, StatementContext statementContext) {
        try {
            Constructor constructor = aClass.getConstructor(ResultSet.class);
            return new ResultSetMapper() {
                @Override
                public Object map(int i, ResultSet resultSet, StatementContext statementContext) throws SQLException {
                    try {
                        return constructor.newInstance(resultSet);
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
