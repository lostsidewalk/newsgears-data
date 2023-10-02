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
@Slf4j
public abstract class AbstractDao<T> {

    /**
     * The application ID retrieved from configuration.
     */
    @Value("${newsgears.data.application-id}")
    protected ApplicationId applicationId;

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
        super();
    }

    /**
     * Gets the name attribute used for identifying entities.
     *
     * @return The name attribute.
     */
    protected String getNameAttribute() {
        return DEFAULT_NAME_ATTRIBUTE;
    }

    /**
     * Gets a list of attributes used during entity insertion.
     *
     * @return A list of attribute names.
     */
    protected List<String> getInsertAttributes() {
        return emptyList();
    }

    /**
     * Configures the parameters for inserting an entity into the database.
     *
     * @param parameters The MapSqlParameterSource to configure.
     * @param entity     The entity to be inserted.
     */
    protected void configureInsertParams(MapSqlParameterSource parameters, T entity) {}

    /**
     * Sets the ID for an entity.
     *
     * @param entity The entity for which to set the ID.
     * @param id     The ID to set.
     */
    protected void setId(@SuppressWarnings("unused") T entity, @SuppressWarnings("unused") long id) {}

    /**
     * Gets the SQL type for a given parameter name.
     *
     * @param parameterName The name of the parameter.
     * @return The SQL type of the parameter.
     */
    protected int getSqlType(String parameterName) {
        return TYPE_UNKNOWN;
    }

    /**
     * Gets a list of attributes used during entity update.
     *
     * @return A list of attribute names.
     */
    protected List<String> getUpdateAttributes() {
        return getInsertAttributes();
    }

    /**
     * Configures the parameters for updating an entity in the database.
     *
     * @param parameters The MapSqlParameterSource to configure.
     * @param entity     The entity to be updated.
     */
    protected void configureUpdateParams(MapSqlParameterSource parameters, T entity) {}

    /**
     * Performs setup operations after construction.
     */
    @SuppressWarnings("unused")
    @PostConstruct
    protected void postConstruct() {
        String tableName = getTableName();
        this.findAllSQL = String.format("select * from %s where application_id = '%s'", tableName, applicationId);
        this.findByNameSQL = String.format("select * from %s where %s = ? and application_id = '%s'", tableName, getNameAttribute(), applicationId);
        this.findByIdSQL = String.format("select * from %s where id = ? and application_id = '%s'", tableName, applicationId);
        this.findAllNamesSQL = String.format("select distinct %s from %s where application_id = '%s'", getNameAttribute(), tableName, applicationId);
        this.deleteByIdSQL = String.format("delete from %s where id = ? and application_id = '%s'", tableName, applicationId);
        this.deleteByNameSQL = String.format("delete from %s where %s = ? and application_id = '%s'", tableName, getNameAttribute(), applicationId);

        // Build insert SQL
        List<String> attributes = getInsertAttributes();
        if (isNotEmpty(attributes)) {
            String insertAttributeNames = join(",", attributes);
            String insertAttributeValueHolders = attributes.stream().map(a -> ":" + a).collect(Collectors.joining(","));
            this.insertSQL = String.format("insert into %s (%s,application_id) values (%s,'%s')", tableName, insertAttributeNames, insertAttributeValueHolders, applicationId);
        } else {
            this.insertSQL = null;
        }

        List<String> updateAttributes = getUpdateAttributes();
        if (isNotEmpty(updateAttributes)) {
            // Build update SQL
            String updateAttributeValueHolders = updateAttributes.stream().map(a -> a + "=:" + a).collect(Collectors.joining(","));
            this.updateSQL = String.format("update %s set %s where id = :id and application_id = '%s'", tableName, updateAttributeValueHolders, applicationId);
        } else {
            this.updateSQL = null;
        }

        setupSQL();
    }

    /**
     * Performs additional setup for SQL queries.
     */
    protected void setupSQL() {}

    /**
     * Retrieves a list of all entities of type T.
     *
     * @return A list of entities.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<T> findAll() throws DataAccessException {
        try {
            return getJdbcTemplate().query(this.findAllSQL, getRowMapper());
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
    public List<String> findAllNames() throws DataAccessException {
        try {
            return getJdbcTemplate().queryForList(this.findAllNamesSQL, String.class);
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
    public T findByName(String ident) throws DataAccessException {
        if (isNotBlank(ident)) {
            try {
                List<T> results = getJdbcTemplate().query(this.findByNameSQL, getRowMapper(), ident);
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
    public T findById(Long id) throws DataAccessException {
        if (id != null) {
            try {
                return getJdbcTemplate().queryForObject(this.findByIdSQL, getRowMapper(), id);
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
    public T add(T t) throws DataAccessException, DataUpdateException, DataConflictException {
        if (this.insertSQL != null) {
            try {
                MapSqlParameterSource parameters = new MapSqlParameterSource();
                configureInsertParams(parameters, t);
                KeyHolder holder = exec(parameters, this.insertSQL, RETURN_GENERATED_KEYS);
                if (holder != null) {
                    Map<String, Object> generatedKeys = holder.getKeys();
                    if (containsKey(generatedKeys, "id")) {
                        Object o = generatedKeys.get("id");
                        if (o != null) {
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
            log.error("INSERT is not supported by this class: {}", this.getClass().getSimpleName());
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
    public T update(T t) throws DataAccessException, DataConflictException {
        if (this.updateSQL != null) {
            try {
                MapSqlParameterSource parameters = new MapSqlParameterSource();
                configureUpdateParams(parameters, t);
                exec(parameters, this.updateSQL, NO_GENERATED_KEYS);
            } catch (DuplicateKeyException e) {
                throw new DataConflictException(getClass().getSimpleName(), "update", e.getMessage(), t);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "update", e.getMessage(), t);
            }
            return t;
       } else {
            log.error("UPDATE is not supported by this class: {}", this.getClass().getSimpleName());
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
                if (sqlType != TYPE_UNKNOWN) {
                    ps.setObject(p, v, sqlType);
                } else {
                    throw new IllegalArgumentException("Parameter has unexpected type: name=" + p + ", type=" + sqlType);
                }
            }
            return ps.getStatement();
        }, keyHolder);

        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "exec", sql, parameters, autoGenerated);
        }
        return autoGenerated == RETURN_GENERATED_KEYS ? keyHolder : null;
    }

    /**
     * Deletes an entity of type T by its ID.
     *
     * @param id The ID of the entity to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void delete(Long id) throws DataAccessException, DataUpdateException {
        if (id != null) {
            int rowsUpdated;
            try {
                Object[] args = new Object[]{id};
                rowsUpdated = getJdbcTemplate().update(this.deleteByIdSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "delete", e.getMessage(), id);
            }
            if (!(rowsUpdated > 0)) {
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
    public void deleteByName(String name) throws DataAccessException, DataUpdateException {
        if (isNotBlank(name)) {
            int rowsUpdated;
            try {
                Object[] args = new Object[]{name};
                rowsUpdated = getJdbcTemplate().update(this.deleteByNameSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "deleteByName", e.getMessage(), name);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "deleteByName", name);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static boolean containsKey(Map<? extends String, ?> m, String key) {
        return m != null && m.containsKey(key);
    }
}
