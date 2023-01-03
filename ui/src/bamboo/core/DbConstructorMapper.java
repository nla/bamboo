package bamboo.core;

import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.mapper.RowMapperFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.Optional;

public class DbConstructorMapper implements RowMapperFactory {
    @Override
    public Optional<RowMapper<?>> build(Type type, ConfigRegistry config) {
        try {
            if (type instanceof Class aClass) {
                Constructor constructor = aClass.getConstructor(ResultSet.class);
                return Optional.of((RowMapper<?>) (resultSet, statementContext) -> {
                    try {
                        return constructor.newInstance(resultSet);
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            return Optional.empty();
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }
}
