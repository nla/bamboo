package bamboo.config;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Allows JPA to save fields of type Path to the database.
 */
@Converter(autoApply = true)
public class JpaPathConverter implements AttributeConverter<Path, String> {
    @Override
    public String convertToDatabaseColumn(Path attribute) {
        return attribute == null ? null : attribute.toString();
    }

    @Override
    public Path convertToEntityAttribute(String dbData) {
        return dbData == null ? null : Paths.get(dbData);
    }
}
