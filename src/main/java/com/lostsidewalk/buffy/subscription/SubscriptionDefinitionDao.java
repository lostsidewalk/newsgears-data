package com.lostsidewalk.buffy.subscription;

import com.google.common.collect.Lists;
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
public class SubscriptionDefinitionDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_URL_SQL = "select exists(select id from subscription_definitions where username = ? and url = ?)";

    @SuppressWarnings("unused")
    public Boolean checkExists(String username, String url) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(CHECK_EXISTS_BY_URL_SQL, new Object[] { username, url }, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), username, url);
        }
    }

    private static final String INSERT_SUBSCRIPTION_DEFINITIONS_SQL =
            "insert into subscription_definitions (" +
                    "queue_id," +
                    "username," +
                    "title," +
                    "img_url," +
                    "url," +
                    "query_type," +
                    "import_schedule," +
                    "query_config" +
                    ") values " +
                    "(?,?,?,?,?,?,?,?::json)";

    @SuppressWarnings("unused")
    public Long add(SubscriptionDefinition subscriptionDefinition) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        KeyHolder keyHolder;
        try {
            keyHolder = new GeneratedKeyHolder();
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_SUBSCRIPTION_DEFINITIONS_SQL, new String[] { "id" });
                        ps.setLong(1, subscriptionDefinition.getQueueId());
                        ps.setString(2, subscriptionDefinition.getUsername());
                        ps.setString(3, subscriptionDefinition.getTitle());
                        ps.setString(4, subscriptionDefinition.getImgUrl());
                        ps.setString(5, subscriptionDefinition.getUrl());
                        ps.setString(6, subscriptionDefinition.getQueryType());
                        ps.setString(7, subscriptionDefinition.getImportSchedule());
                        ps.setString(8, ofNullable(subscriptionDefinition.getQueryConfig()).map(Object::toString).orElse(null));

                        return ps;
                    }, keyHolder);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), subscriptionDefinition);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", subscriptionDefinition);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    @SuppressWarnings("unused")
    public List<Long> add(List<SubscriptionDefinition> subscriptionDefinitions) throws DataAccessException, DataUpdateException {

        List<Long> newIds = Lists.newArrayListWithCapacity(size(subscriptionDefinitions));
        for (SubscriptionDefinition q : subscriptionDefinitions) {
            newIds.add(add(q));
        }

        return newIds;
    }

    final RowMapper<SubscriptionDefinition> SUBSCRIPTION_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long queueId = rs.getLong("queue_id");
        String username = rs.getString("username");
        String title = rs.getString("title");
        String imgUrl = rs.getString("img_url");
        String url = rs.getString("url");
        String queryType = rs.getString("query_type");
        String importSchedule = rs.getString("import_schedule");
        String queryConfig = null;
        PGobject queryConfigObj = (PGobject) rs.getObject("query_config");
        if (queryConfigObj != null) {
            queryConfig = queryConfigObj.getValue();
        }

        SubscriptionDefinition q = SubscriptionDefinition.from(
                queueId,
                username,
                title,
                imgUrl,
                url,
                queryType,
                importSchedule,
                queryConfig
        );
        q.setId(id);

        return q;
    };

    private static final String DELETE_BY_ID_SQL = "delete from subscription_definitions where username = ? and queue_id = ? and id = ?";

    @SuppressWarnings("unused")
    public void deleteById(String username, long queueId, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_ID_SQL, username, queueId, id);
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

    private static final String DELETE_BY_QUEUE_ID_SQL = "delete from subscription_definitions where queue_id = ?";

    @SuppressWarnings("UnusedReturnValue")
    public void deleteByQueueId(Long queueId) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_QUEUE_ID_SQL, queueId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteByQueueId", e.getMessage(), queueId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByQueueId", queueId);
        }
    }

    private static final String FIND_ALL_SQL = "select * from subscription_definitions";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    // Note: this query excludes queues marked for deletion
    private static final String FIND_ALL_ACTIVE_SQL =
            "select * from subscription_definitions q " +
                    "join queue_definitions f on f.id = q.queue_id " +
                    "where f.queue_status = 'ENABLED' and f.is_deleted is false";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findAllActive() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_ACTIVE_SQL, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllActive", e.getMessage());
        }
    }

    private static final String FIND_BY_ID_SQL = "select * from subscription_definitions where username = ? and id = ?";

    @SuppressWarnings("unused")
    public SubscriptionDefinition findById(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_ID_SQL, new Object[] { username, id }, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), username, id);
        }
    }

    private static final String FIND_BY_IDS_SQL_TEMPLATE = "select * from subscription_definitions where username = ? and id in (%s)";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findByIds(String username, List<Long> ids) throws DataAccessException {
        try {
            String inSql = ids.stream()
                    .map(Object::toString)
                    .map(s -> s.replaceAll("[^\\d-]", EMPTY))
                    .collect(joining(","));
            return jdbcTemplate.query(
                    String.format(FIND_BY_IDS_SQL_TEMPLATE, inSql),
                    new Object[]{username},
                    SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByIds", e.getMessage(), username, ids);
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from subscription_definitions where username = ? and queue_id = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findByQueueId(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { username, queueId }, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByQueueId", e.getMessage(), username, queueId);
        }
    }

    private static final String FIND_BY_QUEUE_ID_AND_QUERY_TYPE_SQL = "select * from subscription_definitions where username = ? and queue_id = ? and query_type = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findByQueueId(String username, Long queueId, String queryType) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUEUE_ID_AND_QUERY_TYPE_SQL, new Object[] { username, queueId, queryType }, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByQueueId", e.getMessage(), username, queueId, queryType);
        }
    }

    private static final String FIND_BY_USERNAME_SQL = "select * from subscription_definitions where username = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionDefinition> findByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USERNAME_SQL, new Object[]{username}, SUBSCRIPTION_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUsername", e.getMessage(), username);
        }
    }

    private static final String UPDATE_SUBSCRIPTION_SQL = "update subscription_definitions set " +
            "title = ?, " +
            "img_url = ?, " +
            "url = ?, " +
            "query_type = ?, " +
            "import_schedule = ?, " +
            "query_config = ?::json " +
            "where id = ?";

    @SuppressWarnings("unused")
    public void updateQueries(List<Object[]> queryParams) throws DataAccessException {
        try {
            jdbcTemplate.batchUpdate(UPDATE_SUBSCRIPTION_SQL, queryParams);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateQueries", e.getMessage(), queryParams);
        }
    }

    private static final String UPDATE_IMPORT_SCHEDULE_SQL = "update subscription_definitions set import_schedule = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateImportSchedules(List<Object[]> queryParams) throws DataAccessException {
        try {
            jdbcTemplate.batchUpdate(UPDATE_IMPORT_SCHEDULE_SQL, queryParams);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateImportSchedules", e.getMessage(), queryParams);
        }
    }

    @SuppressWarnings("unused")
    public List<Long> replaceByQueueId(Long queueId, List<SubscriptionDefinition> subscriptionDefinitions) throws DataAccessException, DataUpdateException {
        deleteByQueueId(queueId);
        return add(subscriptionDefinitions);
    }

    private static final String FIND_SUBSCRIPTION_URLS_BY_USERNAME = "select distinct url from subscription_definitions where username = ?";

    @SuppressWarnings("unused")
    public List<String> getSubscriptionUrlsByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.queryForList(FIND_SUBSCRIPTION_URLS_BY_USERNAME, new Object[] { username }, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "getSubscriptionUrlsByUsername", e.getMessage(), username);
        }
    }
}
