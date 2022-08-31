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
abstract class AbstractDao<T> {

    protected abstract JdbcTemplate getJdbcTemplate();

    protected abstract RowMapper<T> getRowMapper();

    private final String tableName;

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

    protected void setId(T entity, long id) {}

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
        this.tableName = tableName;
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

    public List<T> findAll() {
        return getJdbcTemplate().query(this.findAllSQL, getRowMapper());
    }

    public List<String> findAllNames() {
        return getJdbcTemplate().queryForList(this.findAllNamesSQL, String.class);
    }

    public T findByName(String ident) {
        if (isNotBlank(ident)) {
            List<T> results = getJdbcTemplate().query(this.findByNameSQL, getRowMapper(), ident);
            if (!isEmpty(results)) {
                return results.get(0); // name should be unique
            }
        }

        return null;
    }

    public T findById(Long id) {
        if (id != null) {
            return getJdbcTemplate().queryForObject(this.findByIdSQL, getRowMapper(), id); // entity must exist
        }

        return null;
    }

    public T add(T t) {
        if (this.insertSQL != null) {
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
                throw new RuntimeException("Missing key holder, expected non-null key holder when autoGenerated=" + RETURN_GENERATED_KEYS);
            }

            return t;
        } else {
            throw new UnsupportedOperationException("INSERT is not supported by this class" + this.getClass().getSimpleName());
        }
    }

    public T update(T t) {
        if (this.updateSQL != null) {
            MapSqlParameterSource parameters = new MapSqlParameterSource();
            configureUpdateParams(parameters, t);
            exec(parameters, this.updateSQL, NO_GENERATED_KEYS);
            return t;
        } else {
            throw new UnsupportedOperationException("UPDATE is not supported by this class: " + this.getClass().getSimpleName());
        }
    }

    private KeyHolder exec(MapSqlParameterSource parameters, String sql, int autoGenerated) {

        KeyHolder keyHolder = new GeneratedKeyHolder();
        getJdbcTemplate().update(conn -> {
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

        return autoGenerated == RETURN_GENERATED_KEYS ? keyHolder : null;
    }

    public void delete(Long id) {
        if (id != null) {
            Object[] args = new Object[] {id};
            getJdbcTemplate().update(this.deleteByIdSQL, args);
        }
    }

    public void deleteByName(String name) {
        if (isNotBlank(name)) {
            Object[] args = new Object[] {name};
            getJdbcTemplate().update(this.deleteByNameSQL, args);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static boolean containsKey(Map<? extends String, ?> m, String key) {
        return m != null && m.containsKey(key);
    }
}
