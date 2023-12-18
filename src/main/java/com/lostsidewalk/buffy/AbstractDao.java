package com.lostsidewalk.buffy;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Collections.emptyList;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.springframework.jdbc.core.namedparam.SqlParameterSource.TYPE_UNKNOWN;

/**
 * This is an abstract base class for Data Access Objects (DAOs) in the application.
 *
 * @param <T> The type of the entity managed by the DAO.
 */
@SuppressWarnings("OverlyBroadCatchBlock")
@Slf4j
public abstract class AbstractDao<T> {

    /**
     * The application ID retrieved from configuration.
     */
    @Value("${newsgears.data.application-id}")
    ApplicationId applicationId;

    /**
     * Returns the configured application ID.
     *
     * @return the configured ApplicationId object.
     */
    protected final ApplicationId getApplicationId() {
        return applicationId;
    }

    /**
     * Subclasses should provide the JdbcTemplate to use for database operations.
     *
     * @return The JdbcTemplate instance.
     */
    protected abstract JdbcTemplate getJdbcTemplate();

    /**
     * Subclasses should provide the RowMapper for mapping database rows to entity objects.
     *
     * @return The RowMapper instance.
     */
    protected abstract RowMapper<T> getRowMapper();

    /**
     * Subclasses should provide the name of the database table for the entity.
     *
     * @return The name of the database table.
     */
    protected abstract String getTableName();

    /**
     * Get a comprehensive description of all fields in the DAO, for use in building a string representation of this class.
     *
     * @return A string describing the fields in the implementation class.
     */
    protected abstract String getDescription();

    private String findAllSQL;
    private String findByNameSQL;
    private String findByIdSQL;
    private String findAllNamesSQL;
    private String deleteByIdSQL;
    private String deleteByNameSQL;
    private String insertSQL;
    private String updateSQL;
    //
    // attribute accessors
    //
    private static final String DEFAULT_NAME_ATTRIBUTE = "ident";

    /**
     * Default constructor; initializes the object.
     */
    protected AbstractDao() {
    }

    /**
     * Gets the name attribute used for identifying entities.
     *
     * @return The name attribute.
     */
    @SuppressWarnings({"DesignForExtension", "MethodReturnAlwaysConstant"})
    protected String getNameAttribute() {
        return DEFAULT_NAME_ATTRIBUTE;
    }

    /**
     * Gets a list of attributes used during entity insertion.
     *
     * @return A list of attribute names.
     */
    @SuppressWarnings("DesignForExtension")
    protected List<String> getInsertAttributes() {
        return emptyList();
    }

    /**
     * Configures the parameters for inserting an entity into the database.
     *
     * @param parameters The MapSqlParameterSource to configure.
     * @param entity     The entity to be inserted.
     */
    protected abstract void configureInsertParams(MapSqlParameterSource parameters, T entity);

    /**
     * Sets the ID for an entity.
     *
     * @param entity The entity for which to set the ID.
     * @param id     The ID to set.
     */
    protected abstract void setId(@SuppressWarnings("unused") T entity, @SuppressWarnings("unused") long id);

    /**
     * Gets the SQL type for a given parameter name.
     *
     * @param parameterName The name of the parameter.
     * @return The SQL type of the parameter.
     */
    @SuppressWarnings("DesignForExtension")
    protected int getSqlType(String parameterName) {
        return TYPE_UNKNOWN;
    }

    /**
     * Gets a list of attributes used during entity update.
     *
     * @return A list of attribute names.
     */
    @SuppressWarnings("DesignForExtension")
    protected List<String> getUpdateAttributes() {
        return getInsertAttributes();
    }

    /**
     * Configures the parameters for updating an entity in the database.
     *
     * @param parameters The MapSqlParameterSource to configure.
     * @param entity     The entity to be updated.
     */
    protected abstract void configureUpdateParams(MapSqlParameterSource parameters, T entity);

    /**
     * Performs setup operations after construction.
     */
    @SuppressWarnings("unused")
    @PostConstruct
    protected final void postConstruct() {
        String tableName = getTableName();
        findAllSQL = String.format("select * from %s where application_id = '%s'", tableName, applicationId);
        findByNameSQL = String.format("select * from %s where %s = ? and application_id = '%s'", tableName, getNameAttribute(), applicationId);
        findByIdSQL = String.format("select * from %s where id = ? and application_id = '%s'", tableName, applicationId);
        findAllNamesSQL = String.format("select distinct %s from %s where application_id = '%s'", getNameAttribute(), tableName, applicationId);
        deleteByIdSQL = String.format("delete from %s where id = ? and application_id = '%s'", tableName, applicationId);
        deleteByNameSQL = String.format("delete from %s where %s = ? and application_id = '%s'", tableName, getNameAttribute(), applicationId);

        // Build insert SQL
        List<String> attributes = getInsertAttributes();
        if (isNotEmpty(attributes)) {
            String insertAttributeNames = join(",", attributes);
            String insertAttributeValueHolders = attributes.stream().map(a -> ":" + a).collect(Collectors.joining(","));
            insertSQL = String.format("insert into %s (%s,application_id) values (%s,'%s')", tableName, insertAttributeNames, insertAttributeValueHolders, applicationId);
        } else {
            insertSQL = null;
        }

        List<String> updateAttributes = getUpdateAttributes();
        if (isNotEmpty(updateAttributes)) {
            // Build update SQL
            String updateAttributeValueHolders = updateAttributes.stream().map(a -> a + "=:" + a).collect(Collectors.joining(","));
            updateSQL = String.format("update %s set %s where id = :id and application_id = '%s'", tableName, updateAttributeValueHolders, applicationId);
        } else {
            updateSQL = null;
        }

        setupSQL();
    }

    /**
     * Performs additional setup for SQL queries.
     */
    protected abstract void setupSQL();

    /**
     * Retrieves a list of all entities of type T.
     *
     * @return A list of entities.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<T> findAll() throws DataAccessException {
        try {
            return getJdbcTemplate().query(findAllSQL, getRowMapper());
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    /**
     * Retrieves a list of names of all entities of type T.
     *
     * @return A list of entity names.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<String> findAllNames() throws DataAccessException {
        try {
            return getJdbcTemplate().queryForList(findAllNamesSQL, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAllNames", e.getMessage());
        }
    }

    /**
     * Retrieves an entity of type T by its name.
     *
     * @param ident The name or identifier of the entity.
     * @return The entity if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final T findByName(String ident) throws DataAccessException {
        if (isNotBlank(ident)) {
            try {
                List<T> results = getJdbcTemplate().query(findByNameSQL, getRowMapper(), ident);
                if (!isEmpty(results)) {
                    return results.get(0); // name should be unique
                }
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findByName", e.getMessage(), ident);
            }
        }

        return null;
    }

    /**
     * Retrieves an entity of type T by its ID.
     *
     * @param id The ID of the entity.
     * @return The entity if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final T findById(Long id) throws DataAccessException {
        if (null != id) {
            try {
                return getJdbcTemplate().queryForObject(findByIdSQL, getRowMapper(), id);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), id);
            }
        }

        return null;
    }

    /**
     * Adds an entity of type T to the data source.
     *
     * @param t The entity to be added.
     * @return The added entity.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a conflict is detected during the add operation.
     */
    @SuppressWarnings("unused")
    public final T add(T t) throws DataAccessException, DataUpdateException, DataConflictException {
        if (null != insertSQL) {
            try {
                MapSqlParameterSource parameters = new MapSqlParameterSource();
                configureInsertParams(parameters, t);
                KeyHolder holder = exec(parameters, insertSQL, RETURN_GENERATED_KEYS);
                if (null != holder) {
                    Map<String, Object> generatedKeys = holder.getKeys();
                    if (containsKey(generatedKeys, "id")) {
                        Object o = generatedKeys.get("id");
                        if (null != o) {
                            String s = String.valueOf(o);
                            long id = Long.parseLong(s);
                            setId(t, id);
                        }
                    }
                } else {
                    log.error("Missing key holder, expected non-null key holder when autoGenerated=" + RETURN_GENERATED_KEYS);
                    return null;
                }
            } catch (DataUpdateException e) {
                log.warn("Insert probably failed due to: {}", e.getMessage(), e);
                throw new DataUpdateException(getClass().getSimpleName(), "add", t);
            } catch (DuplicateKeyException e) {
                throw new DataConflictException(getClass().getSimpleName(), "add", e.getMessage(), t);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), t);
            }

            return t;
        } else {
            log.error("INSERT is not supported by this class: {}", getClass().getSimpleName());
            return null;
        }
    }

    /**
     * Updates an entity of type T in the data source.
     *
     * @param t The entity to be updated.
     * @return The updated entity.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataConflictException If a conflict is detected during the update operation.
     */
    @SuppressWarnings("unused")
    public final T update(T t) throws DataAccessException, DataConflictException {
        if (null != updateSQL) {
            try {
                MapSqlParameterSource parameters = new MapSqlParameterSource();
                configureUpdateParams(parameters, t);
                exec(parameters, updateSQL, NO_GENERATED_KEYS);
            } catch (DuplicateKeyException e) {
                throw new DataConflictException(getClass().getSimpleName(), "update", e.getMessage(), t);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "update", e.getMessage(), t);
            }
            return t;
       } else {
            log.error("UPDATE is not supported by this class: {}", getClass().getSimpleName());
            return null;
        }
    }

    private KeyHolder exec(MapSqlParameterSource parameters, String sql, int autoGenerated) throws DataUpdateException {

        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rowsUpdated = getJdbcTemplate().update(conn -> {
            NamedParameterStatement ps = new NamedParameterStatement(conn, sql, autoGenerated);
            for (String p : parameters.getParameterNames()) {
                Object v = parameters.getValue(p);
                int sqlType = parameters.getSqlType(p);
                if (TYPE_UNKNOWN == sqlType) {
                    throw new IllegalArgumentException("Parameter has unexpected type: name=" + p + ", type=" + TYPE_UNKNOWN);
                } else {
                    ps.setObject(p, v, sqlType);
                }
            }
            return ps.getStatement();
        }, keyHolder);

        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "exec", sql, parameters, autoGenerated);
        }
        return RETURN_GENERATED_KEYS == autoGenerated ? keyHolder : null;
    }

    /**
     * Deletes an entity of type T by its ID.
     *
     * @param id The ID of the entity to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void delete(Long id) throws DataAccessException, DataUpdateException {
        if (null != id) {
            int rowsUpdated;
            try {
                Object[] args = {id};
                rowsUpdated = getJdbcTemplate().update(deleteByIdSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "delete", e.getMessage(), id);
            }
            if (!(0 < rowsUpdated)) {
                throw new DataUpdateException(getClass().getSimpleName(), "delete", id);
            }
        }
    }

    /**
     * Deletes an entity of type T by its name.
     *
     * @param name The name or identifier of the entity to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void deleteByName(String name) throws DataAccessException, DataUpdateException {
        if (isNotBlank(name)) {
            int rowsUpdated;
            try {
                Object[] args = {name};
                rowsUpdated = getJdbcTemplate().update(deleteByNameSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "deleteByName", e.getMessage(), name);
            }
            if (!(0 < rowsUpdated)) {
                throw new DataUpdateException(getClass().getSimpleName(), "deleteByName", name);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static boolean containsKey(Map<String, ?> map, String key) {
        return null != map && map.containsKey(key);
    }

    @Override
    public final String toString() {
        return "{" +
                "applicationId=" + applicationId +
                ", findAllSQL='" + findAllSQL + '\'' +
                ", findByNameSQL='" + findByNameSQL + '\'' +
                ", findByIdSQL='" + findByIdSQL + '\'' +
                ", findAllNamesSQL='" + findAllNamesSQL + '\'' +
                ", deleteByIdSQL='" + deleteByIdSQL + '\'' +
                ", deleteByNameSQL='" + deleteByNameSQL + '\'' +
                ", insertSQL='" + insertSQL + '\'' +
                ", updateSQL='" + updateSQL + '\'' +
                ", description={" + getDescription() + '}' +
                '}';
    }
}
