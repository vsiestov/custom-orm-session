package orm;

import lombok.SneakyThrows;
import orm.action.DeleteAction;
import orm.action.InsertAction;
import orm.action.OrmAction;
import orm.action.UpdateAction;
import orm.metadata.EntityKey;
import orm.metadata.TypeMetadata;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Session {
    private final String FIND_ALL_QUERY = "SELECT * FROM %s";
    private final String FIND_ONE_QUERY = "SELECT * FROM %s WHERE %s = ?";
    private final String UPDATE_QUERY = "UPDATE %s SET %s WHERE %s = ?";
    private final String INSERT_QUERY = "INSERT INTO %s (%s) VALUES (%s)";
    private final String DELETE_QUERY = "DELETE FROM %s WHERE %s = ?";

    private final Map<Class<?>, TypeMetadata> types = new HashMap<>();
    private final Map<EntityKey, Object> cache = new HashMap<>();
    private final Map<EntityKey, Object[]> snapshot = new HashMap<>();
    private final List<OrmAction> actions = new ArrayList<>();
    private final Connection connection;

    public Session(DataSource dataSource) throws SQLException {
        this.connection = dataSource.getConnection();
        this.connection.setAutoCommit(false);
    }

    @SneakyThrows
    private <T> T parseResultSet(Class<T> type, ResultSet resultSet) {
        TypeMetadata<T> typeMetadata = types.computeIfAbsent(type, TypeMetadata::new);

        return typeMetadata.parse(resultSet);
    }

    @SneakyThrows
    private void executeSelectQuery(Consumer<ResultSet> resultSetConsumer, String sql, Object... params) {
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

    @SneakyThrows
    private void executeUpdateQuery(String sql, Object... params) {
        System.out.printf("SQL: %s%n", sql);

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }

            preparedStatement.executeUpdate();
        }
    }

    @SneakyThrows
    private void updateEntity(Object entity) {
        TypeMetadata<?> typeMetadata = types.computeIfAbsent(entity.getClass(), TypeMetadata::new);

        Map<String, Object> columns = typeMetadata.getColumns(entity);
        List<String> params = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        columns.forEach((s, o) -> {
            params.add(String.format("%s = ?", s));
            values.add(o);
        });

        values.add(typeMetadata.getIdValue(entity));

        actions.add(new UpdateAction(
            String.format(
                UPDATE_QUERY,
                typeMetadata.getTableName(),
                String.join(", ", params),
                typeMetadata.getIdentityName()
            ),
            values.toArray()
        ));
    }

    @SneakyThrows
    public void persist(Object entity) {
        TypeMetadata<?> typeMetadata = types.computeIfAbsent(entity.getClass(), TypeMetadata::new);

        Map<String, Object> columns = typeMetadata.getColumns(entity);
        List<String> params = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        columns.forEach((s, o) -> {
            params.add(s);
            values.add(o);
        });

        actions.add(new InsertAction(
            String.format(
                INSERT_QUERY,
                typeMetadata.getTableName(),
                String.join(", ", params),
                params.stream()
                    .map(f -> "?").collect(Collectors.joining(", "))
            ),
            values.toArray()
        ));
    }

    @SneakyThrows
    public void remove(Object entity) {
        TypeMetadata<?> typeMetadata = types.computeIfAbsent(entity.getClass(), TypeMetadata::new);

        actions.add(new DeleteAction(
            String.format(DELETE_QUERY, typeMetadata.getTableName(), typeMetadata.getIdentityName()),
            new Object[]{typeMetadata.getIdValue(entity)}
        ));
    }

    private <T> List<T> findAll(Class<T> type) {
        List<T> result = new ArrayList<>();
        TypeMetadata<T> typeMetadata = types.computeIfAbsent(type, TypeMetadata::new);

        executeSelectQuery(
            rs -> result.add(parseResultSet(type, rs)),
            String.format(FIND_ALL_QUERY, typeMetadata.getTableName())
        );

        return result;
    }

    private <T> T findOne(Class<T> type, Object id) {
        List<T> result = new ArrayList<>();
        TypeMetadata<T> typeMetadata = types.computeIfAbsent(type, TypeMetadata::new);

        executeSelectQuery(
            rs -> result.add(parseResultSet(type, rs)),
            String.format(FIND_ONE_QUERY, typeMetadata.getTableName(), typeMetadata.getIdentityName()),
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
        TypeMetadata<?> typeMetadata = types.computeIfAbsent(value.getClass(), TypeMetadata::new);

        return typeMetadata.getColumns(value)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toArray();
    }

    private <T> T cacheEntity(Class<T> type, Object id, T result) {
        var key = new EntityKey(type, id);
        cache.put(key, result);
        snapshot.put(key, getObjectValues(result));

        return result;
    }

    private <T> List<T> cacheEntityList(Class<T> type, List<T> result) {
        TypeMetadata<T> typeMetadata = types.computeIfAbsent(type, TypeMetadata::new);

        result.forEach(item -> cacheEntity(type, typeMetadata.getIdValue(item), item));

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
            .toList();
    }

    public <T> List<T> find(Class<T> type) {
        flush();

        return cacheEntityList(type, findAll(type));
    }

    public <T> T find(Class<T> type, Object id) {
        flush();

        return getEntityFromCache(type, id)
            .orElseGet(() -> cacheEntity(type, id, findOne(type, id)));
    }

    public void close() throws SQLException {
        try {
            getAffectedEntities().forEach(this::updateEntity);

            flush();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        } finally {
            connection.close();
        }
    }

    public void flush() {
        if (actions.isEmpty()) {
            return;
        }

        actions.stream()
            .filter(a -> a.getClass().equals(InsertAction.class))
            .forEach(a -> executeUpdateQuery(a.getSql(), a.getParams()));

        actions.stream()
            .filter(a -> a.getClass().equals(UpdateAction.class))
            .forEach(a -> executeUpdateQuery(a.getSql(), a.getParams()));

        actions.stream()
            .filter(a -> a.getClass().equals(DeleteAction.class))
            .forEach(a -> executeUpdateQuery(a.getSql(), a.getParams()));

        actions.clear();
    }
}
