package com.lostsidewalk.buffy.query;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.collections4.CollectionUtils.size;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@Slf4j
@Component
public class QueryDefinitionDao {

    private static final Gson GSON = new Gson();

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from query_definitions where id = '%s')";

    @SuppressWarnings("unused")
    public Boolean checkExists(String id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_EXISTS_BY_ID_SQL_TEMPLATE, id);
            return jdbcTemplate.queryForObject(sql, null, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), id);
        }
    }

    private static final String INSERT_QUERY_DEFINITIONS_SQL =
            "insert into query_definitions (" +
                    "feed_id," +
                    "username," +
                    "query_title," +
                    "query_image_url," +
                    "query_text," +
                    "query_type," +
                    "query_config" +
                    ") values " +
                    "(?,?,?,?,?,?,?::json)";

    @SuppressWarnings("unused")
    public Long add(QueryDefinition queryDefinition) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        KeyHolder keyHolder;
        try {
            keyHolder = new GeneratedKeyHolder();
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_QUERY_DEFINITIONS_SQL, new String[] { "id" });
                        ps.setLong(1, queryDefinition.getFeedId());
                        ps.setString(2, queryDefinition.getUsername());
                        ps.setString(3, queryDefinition.getQueryTitle());
                        ps.setString(4, queryDefinition.getQueryImageUrl());
                        ps.setString(5, queryDefinition.getQueryText());
                        ps.setString(6, queryDefinition.getQueryType());
                        ps.setString(7, ofNullable(queryDefinition.getQueryConfig()).map(GSON::toJson).orElse(null));

                        return ps;
                    }, keyHolder);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queryDefinition);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queryDefinition);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    @SuppressWarnings("unused")
    public List<Long> add(List<QueryDefinition> queryDefinitions) throws DataAccessException, DataUpdateException {

        List<Long> newIds = Lists.newArrayListWithCapacity(size(queryDefinitions));
        for (QueryDefinition q : queryDefinitions) {
            newIds.add(add(q));
        }

        return newIds;
    }

    final RowMapper<QueryDefinition> QUERY_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long feedId = rs.getLong("feed_id");
        String username = rs.getString("username");
        String queryTitle = rs.getString("query_title");
        String queryImageUrl = rs.getString("query_image_url");
        String queryText = rs.getString("query_text");
        String queryType = rs.getString("query_type");
        String queryConfig = null;
        PGobject queryConfigObj = (PGobject) rs.getObject("query_config");
        if (queryConfigObj != null) {
            queryConfig = queryConfigObj.getValue();
        }

        QueryDefinition q = QueryDefinition.from(
                feedId,
                username,
                queryTitle,
                queryImageUrl,
                queryText,
                queryType,
                queryConfig
        );
        q.setId(id);

        return q;
    };

    private static final String DELETE_BY_ID_SQL = "delete from query_definitions where id = ?";

    @SuppressWarnings("unused")
    public void deleteById(long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_ID_SQL, id);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", id);
        }
    }

    @SuppressWarnings("unused")
    public void deleteQueries(List<Long> ids) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> args = ids.stream().map(i -> new Object[]{i}).collect(toList());
            rowsUpdated = stream(jdbcTemplate.batchUpdate(DELETE_BY_ID_SQL, args)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteQueries", e.getMessage(), ids);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteQueries", ids);
        }
    }

    private static final String DELETE_BY_FEED_ID_SQL = "delete from query_definitions where feed_id = ?";

    @SuppressWarnings("UnusedReturnValue")
    public void deleteByFeedId(Long feedId) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_FEED_ID_SQL, feedId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteByFeedId", e.getMessage(), feedId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByFeedId", feedId);
        }
    }

    private static final String FIND_ALL_SQL = "select * from query_definitions";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_ALL_ACTIVE_SQL =
            "select * from query_definitions q " +
                    "join feed_definitions f on f.id = q.feed_id " +
                    "where f.feed_status = 'ENABLED' and f.is_deleted is false";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findAllActive() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_ACTIVE_SQL, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllActive", e.getMessage());
        }
    }

    private static final String FIND_BY_ID_SQL = "select * from query_definitions where username = ? and id = ?";

    @SuppressWarnings("unused")
    public QueryDefinition findById(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_ID_SQL, new Object[] { username, id }, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), username, id);
        }
    }

    private static final String FIND_BY_IDS_SQL_TEMPLATE = "select * from query_definitions where username = ? and id in (%s)";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findByIds(String username, List<Long> ids) throws DataAccessException {
        try {
            String inSql = ids.stream()
                    .map(Object::toString)
                    .map(s -> s.replaceAll("[^\\d-]", EMPTY))
                    .collect(joining(","));
            return jdbcTemplate.query(
                    String.format(FIND_BY_IDS_SQL_TEMPLATE, inSql),
                    new Object[]{username},
                    QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByIds", e.getMessage(), username, ids);
        }
    }

    private static final String FIND_BY_FEED_ID_SQL = "select * from query_definitions where username = ? and feed_id = ?";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findByFeedId(String username, Long feedId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_FEED_ID_SQL, new Object[] { username, feedId }, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, feedId);
        }
    }

    private static final String FIND_BY_FEED_ID_AND_QUERY_TYPE_SQL = "select * from query_definitions where username = ? and feed_id = ? and query_type = ?";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findByFeedId(String username, Long feedId, String queryType) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_FEED_ID_AND_QUERY_TYPE_SQL, new Object[] { username, feedId, queryType }, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, feedId, queryType);
        }
    }

    private static final String FIND_BY_USERNAME_SQL = "select * from query_definitions where username = ?";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USERNAME_SQL, new Object[]{username}, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUsername", e.getMessage(), username);
        }
    }

    private static final String UPDATE_QUERY_SQL = "update query_definitions set " +
            "query_title = ?, " +
            "query_image_url = ?, " +
            "query_text = ?, " +
            "query_type = ?, " +
            "query_config = ?::json " +
            "where id = ?";

    @SuppressWarnings("unused")
    public void updateQueries(List<Object[]> queryParams) throws DataAccessException {
        try {
            jdbcTemplate.batchUpdate(UPDATE_QUERY_SQL, queryParams);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateQueries", e.getMessage(), queryParams);
        }
    }

    @SuppressWarnings("unused")
    public List<Long> replaceByFeedId(Long feedId, List<QueryDefinition> queryDefinitions) throws DataAccessException, DataUpdateException {
        deleteByFeedId(feedId);
        return add(queryDefinitions);
    }
}
