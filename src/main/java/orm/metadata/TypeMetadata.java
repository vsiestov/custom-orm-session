package orm.metadata;

import orm.annotation.Column;
import orm.annotation.Id;
import orm.annotation.Table;
import orm.exception.EntityIdentityNotFound;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TypeMetadata<T> {
    private final Class<T> type;
    private final Field[] fields;
    private final String identityName;
    private final Field identityField;
    private final String tableName;

    public TypeMetadata(Class<T> type) {
        this.type = type;
        this.fields = type.getDeclaredFields();
        this.identityField = getIdField(fields);
        this.identityName = identityField.getAnnotation(Id.class).name();
        this.tableName = getTableName(type);
    }

    private Field getIdField(Field[] fields) {
        return Arrays.stream(fields)
            .filter(field -> field.getAnnotation(Id.class) != null)
            .findFirst()
            .orElseThrow(() -> new EntityIdentityNotFound(
                String.format("Entity %s does not include @Id annotation", type.getSimpleName())
            ));
    }

    private String getTableName(Class<?> type) {
        Table tableNameAnnotation = type.getAnnotation(Table.class);

        if (tableNameAnnotation != null && tableNameAnnotation.name() != null) {
            return tableNameAnnotation.name();
        }

        return type.getSimpleName();
    }

    private String getColumnName(Field field) {
        Column fieldAnnotation = field.getAnnotation(Column.class);

        if (fieldAnnotation != null && fieldAnnotation.name() != null) {
            return fieldAnnotation.name();
        }

        return field.getName();
    }

    public Map<String, Object> getColumns(Object entity) {
        Map<String, Object> result = new HashMap<>();

        for (Field field : fields) {
            if (field.getAnnotation(Id.class) != null || field.getAnnotation(Column.class) == null) {
                continue;
            }

            field.setAccessible(true);

            try {
                Object o = field.get(entity);

                if (o == null) {
                    continue;
                }

                result.put(getColumnName(field), o);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public String getIdentityName() {
        return identityName;
    }

    public String getTableName() {
        return tableName;
    }

    public Object getIdValue(Object entity) {
        identityField.setAccessible(true);

        try {
            return identityField.get(entity);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return null;
    }

    public T parse(ResultSet resultSet) throws InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
        Constructor<T> constructor = (Constructor<T>) type.getDeclaredConstructors()[0];
        T instance = constructor.newInstance();

        for (Field field : fields) {
            field.setAccessible(true);
            field.set(instance, resultSet.getObject(getColumnName(field)));
        }

        return instance;
    }
}
