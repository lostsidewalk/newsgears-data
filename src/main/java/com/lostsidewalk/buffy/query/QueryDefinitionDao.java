package com.lostsidewalk.buffy.query;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

@Slf4j
@Component
public class QueryDefinitionDao {

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
                    "query_text," +
                    "query_type," +
                    "query_config" +
                    ") values " +
                    "(?,?,?,?,?,?::json)";

    @SuppressWarnings("unused")
    public void add(QueryDefinition queryDefinition) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(INSERT_QUERY_DEFINITIONS_SQL,
                    queryDefinition.getFeedId(),
                    queryDefinition.getUsername(),
                    queryDefinition.getQueryTitle(),
                    queryDefinition.getQueryText(),
                    queryDefinition.getQueryType(),
                    queryDefinition.getQueryConfig()
            );
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queryDefinition);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queryDefinition);
        }
    }

    @SuppressWarnings("unused")
    public void add(List<QueryDefinition> queryDefinitions) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> args = queryDefinitions.stream()
                    .map(q -> new Object[] {
                            q.getFeedId(),
                            q.getUsername(),
                            q.getQueryTitle(),
                            q.getQueryText(),
                            q.getQueryType(),
                            q.getQueryConfig(),
                    }).collect(toList());
            rowsUpdated = Arrays.stream(jdbcTemplate.batchUpdate(INSERT_QUERY_DEFINITIONS_SQL, args)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queryDefinitions);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queryDefinitions);
        }
    }

    final RowMapper<QueryDefinition> QUERY_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long feedId = rs.getLong("feed_id");
        String username = rs.getString("username");
        String queryTitle = rs.getString("query_title");
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
                    "where f.feed_status = 'ENABLED'";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findAllActive() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_ACTIVE_SQL, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllActive", e.getMessage());
        }
    }

    private static final String FIND_BY_FEED_ID_SQL = "select * from query_definitions where username = ? and feed_id = ?";

    @SuppressWarnings("unused")
    public List<QueryDefinition> findByFeedId(String username, Long feedId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_FEED_ID_SQL, new Object[] { username, feedId }, QUERY_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedIdent", e.getMessage(), username, feedId);
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
    public void replaceByFeedId(Long feedId, List<QueryDefinition> queryDefinitions) throws DataAccessException, DataUpdateException {
        deleteByFeedId(feedId);
        add(queryDefinitions);
    }
}
