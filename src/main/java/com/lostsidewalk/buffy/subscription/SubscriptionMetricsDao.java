package com.lostsidewalk.buffy.subscription;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;

@Slf4j
@Component
public class SubscriptionMetricsDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String INSERT_SUBSCRIPTION_METRICS_SQL =
            "insert into subscription_metrics (" +
                    "subscription_id," +
                    "http_status_code," +
                    "http_status_message," +
                    "redirect_feed_url," +
                    "redirect_http_status_code," +
                    "redirect_http_status_message," +
                    "import_timestamp," +
                    "import_schedule," +
                    "import_ct," +
                    "persist_ct," +
                    "skip_ct," +
                    "archive_ct," +
                    "error_type," +
                    "error_detail" +
                    ") values " +
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @SuppressWarnings("unused")
    public void add(SubscriptionMetrics queryMetrics) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(INSERT_SUBSCRIPTION_METRICS_SQL,
                    queryMetrics.getSubscriptionId(),
                    queryMetrics.getHttpStatusCode(),
                    queryMetrics.getHttpStatusMessage(),
                    queryMetrics.getRedirectFeedUrl(),
                    queryMetrics.getRedirectHttpStatusCode(),
                    queryMetrics.getRedirectHttpStatusMessage(),
                    queryMetrics.getImportTimestamp(),
                    queryMetrics.getImportSchedule(),
                    queryMetrics.getImportCt(),
                    queryMetrics.getPersistCt(),
                    queryMetrics.getSkipCt(),
                    queryMetrics.getArchiveCt(),
                    ofNullable(queryMetrics.getErrorType()).map(Enum::name).orElse(null),
                    queryMetrics.getErrorDetail()
            );
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queryMetrics);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queryMetrics);
        }
    }

    @SuppressWarnings("unused")
    final RowMapper<SubscriptionMetrics> SUBSCRIPTION_METRICS_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long subscriptionId = rs.getLong("subscription_id");
        Integer httpStatusCode = rs.wasNull() ? null : rs.getInt("http_status_code");
        // http status message
        String httpStatusMessage = rs.getString("http_status_message");
        // redirect feed URL
        String redirectFeedUrl = rs.getString("redirect_feed_url");
        // redirect http status code
        Integer redirectHttpStatusCode = rs.wasNull() ? null : rs.getInt("redirect_http_status_code");
        // redirect http status message
        String redirectHttpStatusMessage = rs.getString("redirect_http_status_message");
        // import timestamp
        Timestamp importTimestamp = rs.getTimestamp("import_timestamp");
        // import schedule
        String importSchedule = rs.getString("import_schedule");
        // import ct
        Integer importCt = rs.getInt("import_ct");
        // persist ct
        Integer persistCt = rs.getInt("persist_ct");
        // skip ct
        Integer skipCt = rs.getInt("skip_ct");
        // archive ct
        Integer archiveCt = rs.getInt("archive_ct");
        // error type
        String errorType = rs.getString("error_type");
        // error detail
        String errorDetail = rs.getString("error_detail");

        SubscriptionMetrics q = SubscriptionMetrics.from(
                subscriptionId,
                httpStatusCode,
                httpStatusMessage,
                redirectFeedUrl,
                redirectHttpStatusCode,
                redirectHttpStatusMessage,
                importTimestamp,
                importSchedule,
                importCt
        );
        q.setId(id);
        q.setPersistCt(persistCt);
        q.setSkipCt(skipCt);
        q.setArchiveCt(archiveCt);
        q.setErrorType(ofNullable(errorType).map(e -> SubscriptionMetrics.QueryExceptionType.valueOf(errorType)).orElse(null)); // TODO: safety
        q.setErrorDetail(errorDetail);

        return q;
    };

    private static final String FIND_ALL_SQL = "select * from subscription_metrics";

    @SuppressWarnings("unused")
    public List<SubscriptionMetrics> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.queue_id = ? and qd.username = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionMetrics> findByQueueId(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { queueId, username }, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, queueId);
        }
    }

    private static final String FIND_BY_SUBSCRIPTION_ID_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.id = ? and qd.username = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionMetrics> findBySubscriptionId(String username, Long subscriptionId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_SUBSCRIPTION_ID_SQL, new Object[] { subscriptionId, username }, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findBySubscriptionId", e.getMessage(), username, subscriptionId);
        }
    }

    private static final String FIND_BY_USERNAME_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.username = ?";

    @SuppressWarnings("unused")
    public List<SubscriptionMetrics> findByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USERNAME_SQL, new Object[]{username}, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUsername", e.getMessage(), username);
        }
    }

    private static final String FIND_LATEST_BY_USERNAME_SQL = "select qd.queue_id,max(qm.import_timestamp) as max_import_timestamp from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.username = ? group by qd.queue_id";

    @SuppressWarnings("unused")
    public Map<Long, Timestamp> findLatestByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_LATEST_BY_USERNAME_SQL, new Object[]{username}, rs -> {
                Map<Long, Timestamp> m = new HashMap<>();
                while (rs.next()) {
                    Long feedId = rs.getLong("queue_id");
                    Timestamp maxImportTimestamp = rs.getTimestamp("max_import_timestamp");
                    m.put(feedId, maxImportTimestamp);
                }
                return m;
            });
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findLatestByUsername", e.getMessage(), username);
        }
    }

    //
    //
    //

    private static final String PURGE_ORPHANED = "delete from subscription_metrics where id in (select qm.id from subscription_metrics qm left join subscription_definitions qd on qd.id = qm.subscription_id where qd.id is null)";

    @SuppressWarnings("unused")
    public int purgeOrphaned() throws DataAccessException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(PURGE_ORPHANED);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "purgeOrphaned", e.getMessage());
        }

        return rowsUpdated;
    }
}
