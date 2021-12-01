package orm;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import orm.annotation.Column;
import orm.annotation.Id;
import orm.annotation.Table;
import orm.exception.EntityIdentityNotFound;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Session {
    private final String FIND_ALL_QUERY = "SELECT * FROM %s";
    private final String FIND_ONE_QUERY = "SELECT * FROM %s WHERE %s = ?";
    private final String UPDATE_QUERY = "UPDATE %s SET %s WHERE %s = ?";
    private final Map<EntityKey, Object> cache = new HashMap<>();
    private final Map<EntityKey, Object[]> snapshot = new HashMap<>();

    private final DataSource dataSource;

    private String getIdField(Class<?> type) {
        return Arrays.stream(type.getDeclaredFields())
            .filter(field -> field.getAnnotation(Id.class) != null)
            .findFirst()
            .map(field -> field.getAnnotation(Id.class).name())
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

    @SneakyThrows
    private <T> T parseResultSet(Class<T> type, ResultSet resultSet) {
        Constructor<T> constructor = (Constructor<T>) type.getDeclaredConstructors()[0];
        T instance = constructor.newInstance();

        for (Field field : type.getDeclaredFields()) {
            field.setAccessible(true);
            field.set(instance, resultSet.getObject(getColumnName(field)));
        }

        return instance;
    }

    @SneakyThrows
    private void executeSelectQuery(Consumer<ResultSet> resultSetConsumer, String sql, Object... params) {
        try (Connection connection = dataSource.getConnection()) {

            System.out.printf("SQL: %s%n", sql);

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }

                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    @SneakyThrows
    private void executeUpdateQuery(String sql, Object... params) {
        try (Connection connection = dataSource.getConnection()) {

            System.out.printf("SQL: %s%n", sql);

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }

                preparedStatement.executeUpdate();
            }
        }
    }

    @SneakyThrows
    private void updateEntity(Object entity) {
        Class<?> type = entity.getClass();
        String identityName = getIdField(type);
        Field identityField = type.getDeclaredField(identityName);
        Field[] fields = type.getDeclaredFields();
        List<Object> params = new ArrayList<>();

        String sqlUpdateParams = Arrays.stream(fields)
            .filter(field -> !field.getName().equals(identityName))
            .map(field -> {
                field.setAccessible(true);

                try {
                    params.add(field.get(entity));
                    return String.format("%s = ?", getColumnName(field));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.joining(", "));

        identityField.setAccessible(true);
        params.add(identityField.get(entity));

        executeUpdateQuery(String.format(UPDATE_QUERY, getTableName(type), sqlUpdateParams, identityName), params.toArray());
    }

    private <T> List<T> findAll(Class<T> type) {
        List<T> result = new ArrayList<>();

        executeSelectQuery(
            rs -> result.add(parseResultSet(type, rs)),
            String.format(FIND_ALL_QUERY, getTableName(type))
        );

        return result;
    }

    private <T> T findOne(Class<T> type, Object id) {
        List<T> result = new ArrayList<>();

        executeSelectQuery(
            rs -> result.add(parseResultSet(type, rs)),
            String.format(FIND_ONE_QUERY, getTableName(type), getIdField(type)),
            id
        );

        if (result.isEmpty()) {
            return null;
        }

        return result.get(0);
    }

    private <T> Optional<T> getEntityFromCache(Class<T> type, Object id) {
        var key = new EntityKey(type, id);
        var cachedResult = cache.get(key);

        return Optional.ofNullable(cachedResult)
            .map(type::cast);
    }

    private Object[] getObjectValues(Object value) {
        Class<?> type = value.getClass();

        return Arrays.stream(type.getDeclaredFields())
            .sorted(Comparator.comparing(Field::getName))
            .map(field -> {
                field.setAccessible(true);
                try {
                    return field.get(value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                return null;
            })
            .filter(Objects::nonNull)
            .toArray();
    }

    private <T> T cacheEntity(Class<T> type, Object id, T result) {
        var key = new EntityKey(type, id);
        cache.put(key, result);
        snapshot.put(key, getObjectValues(result));

        return result;
    }

    private <T> List<T> cacheEntityList(Class<T> type, List<T> result) {
        result.forEach(item -> {
            try {
                String fieldName = getIdField(type);
                Field identityField = type.getDeclaredField(fieldName);
                identityField.setAccessible(true);
                cacheEntity(type, identityField.get(item), item);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        });

        return result;
    }

    private boolean hasChanges(Map.Entry<EntityKey, Object> key) {
        Object[] currentValues = getObjectValues(cache.get(key.getKey()));
        Object[] snapshotValues = snapshot.get(key.getKey());

        int count = currentValues.length;

        for (int i = 0; i < count; i++) {
            if (!currentValues[i].equals(snapshotValues[i])) {
                return true;
            }
        }

        return false;
    }

    private List<?> getAffectedEntities() {
        return cache.entrySet().stream()
            .filter(this::hasChanges)
            .map(entityKeyObjectEntry -> cache.get(entityKeyObjectEntry.getKey()))
            .collect(Collectors.toList());
    }

    public <T> List<T> find(Class<T> type) {
        return cacheEntityList(type, findAll(type));
    }

    public <T> T find(Class<T> type, Object id) {
        return getEntityFromCache(type, id)
            .orElseGet(() -> cacheEntity(type, id, findOne(type, id)));
    }

    public void close() {
        getAffectedEntities().forEach(this::updateEntity);
    }
}
