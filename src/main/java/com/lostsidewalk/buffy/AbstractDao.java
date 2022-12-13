package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public abstract class AbstractDao<T> {

    protected abstract JdbcTemplate getJdbcTemplate();

    protected abstract RowMapper<T> getRowMapper();

    private final String findAllSQL;
    private final String findByNameSQL;
    private final String findByIdSQL;
    private final String findAllNamesSQL;
    private final String deleteByIdSQL;
    private final String deleteByNameSQL;
    private final String insertSQL;
    private final String updateSQL;
    //
    // attribute accessors
    //
    private static final String DEFAULT_NAME_ATTRIBUTE = "ident";

    protected String getNameAttribute() {
        return DEFAULT_NAME_ATTRIBUTE;
    }

    protected List<String> getInsertAttributes() {
        return emptyList();
    }
    //
    // insert support methods
    //
    protected void configureInsertParams(MapSqlParameterSource parameters, T entity) {}

    protected void setId(@SuppressWarnings("unused") T entity, @SuppressWarnings("unused") long id) {}

    protected int getSqlType(String parameterName) {
        return TYPE_UNKNOWN;
    }
    //
    // update support methods
    //
    protected List<String> getUpdateAttributes() {
        return getInsertAttributes();
    }

    protected void configureUpdateParams(MapSqlParameterSource parameters, T entity) {}

    protected AbstractDao(String tableName) {
        this.findAllSQL = String.format("select * from %s", tableName);
        this.findByNameSQL = String.format("select * from %s where %s = ?", tableName, getNameAttribute());
        this.findByIdSQL = String.format("select * from %s where id = ?", tableName);
        this.findAllNamesSQL = String.format("select distinct %s from %s", getNameAttribute(), tableName);
        this.deleteByIdSQL = String.format("delete from %s where id = ?", tableName);
        this.deleteByNameSQL = String.format("delete from %s where %s = ?", tableName, getNameAttribute());

        // build insert SQL
        List<String> attributes = getInsertAttributes();
        if (isNotEmpty(attributes)) {
            String insertAttributeNames = join(",", attributes);
            String insertAttributeValueHolders = attributes.stream().map(a -> ":" + a).collect(Collectors.joining(","));
            this.insertSQL = String.format("insert into %s (%s) values (%s)", tableName, insertAttributeNames, insertAttributeValueHolders);
        } else {
            this.insertSQL = null;
        }

        List<String> updateAttributes = getUpdateAttributes();
        if (isNotEmpty(updateAttributes)) {
            // build update SQL
            String updateAttributeValueHolders = updateAttributes.stream().map(a -> a + "=:" + a).collect(Collectors.joining(","));
            this.updateSQL = String.format("update %s set %s where id = :id", tableName, updateAttributeValueHolders);
        } else {
            this.updateSQL = null;
        }

    }

    @SuppressWarnings("unused")
    public List<T> findAll() throws DataAccessException {
        try {
            return getJdbcTemplate().query(this.findAllSQL, getRowMapper());
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    public List<String> findAllNames() throws DataAccessException {
        try {
            return getJdbcTemplate().queryForList(this.findAllNamesSQL, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllNames", e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    public T findByName(String ident) throws DataAccessException {
        if (isNotBlank(ident)) {
            try {
                List<T> results = getJdbcTemplate().query(this.findByNameSQL, getRowMapper(), ident);
                if (!isEmpty(results)) {
                    return results.get(0); // name should be unique
                }
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "findByName", e.getMessage(), ident);
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    public T findById(Long id) throws DataAccessException {
        if (id != null) {
            try {
                return getJdbcTemplate().queryForObject(this.findByIdSQL, getRowMapper(), id);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), id);
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    public T add(T t) throws DataAccessException, DataUpdateException {
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
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), t);
            }

            return t;
        } else {
            log.error("INSERT is not supported by this class: {}", this.getClass().getSimpleName());
            return null;
        }
    }

    @SuppressWarnings("unused")
    public T update(T t) throws DataAccessException {
        if (this.updateSQL != null) {
            try {
                MapSqlParameterSource parameters = new MapSqlParameterSource();
                configureUpdateParams(parameters, t);
                exec(parameters, this.updateSQL, NO_GENERATED_KEYS);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
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

    @SuppressWarnings("unused")
    public void delete(Long id) throws DataAccessException, DataUpdateException {
        if (id != null) {
            int rowsUpdated;
            try {
                Object[] args = new Object[]{id};
                rowsUpdated = getJdbcTemplate().update(this.deleteByIdSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "delete", e.getMessage(), id);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "delete", id);
            }
        }
    }

    @SuppressWarnings("unused")
    public void deleteByName(String name) throws DataAccessException, DataUpdateException {
        if (isNotBlank(name)) {
            int rowsUpdated;
            try {
                Object[] args = new Object[]{name};
                rowsUpdated = getJdbcTemplate().update(this.deleteByNameSQL, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
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
