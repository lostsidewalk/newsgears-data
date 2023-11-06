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

/**
 * Data access object for managing subscription metrics in the application.
 */
@SuppressWarnings({"deprecation", "OverlyBroadCatchBlock"})
@Slf4j
@Component
public class SubscriptionMetricsDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Default constructor; initializes the object.
     */
    SubscriptionMetricsDao() {
    }

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

    /**
     * Adds a subscription metrics record to the database.
     *
     * @param queryMetrics The subscription metrics data to be added.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the data update operation fails.
     */
    @SuppressWarnings("unused")
    public final void add(SubscriptionMetrics queryMetrics) throws DataAccessException, DataUpdateException {
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
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queryMetrics);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queryMetrics);
        }
    }

    @SuppressWarnings("unused")
    private final RowMapper<SubscriptionMetrics> SUBSCRIPTION_METRICS_ROW_MAPPER = (rs, rowNum) -> {
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

    /**
     * Retrieves all subscription metrics records from the database.
     *
     * @return A list of SubscriptionMetrics objects containing the subscription metrics data.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<SubscriptionMetrics> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.queue_id = ? and qd.username = ?";

    /**
     * Retrieves subscription metrics records associated with a specific queue and username from the database.
     *
     * @param username The username associated with the subscription metrics records.
     * @param queueId  The ID of the queue to filter the records by.
     * @return A list of SubscriptionMetrics objects containing the subscription metrics data for the specified queue and username.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<SubscriptionMetrics> findByQueueId(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { queueId, username }, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, queueId);
        }
    }

    private static final String FIND_BY_SUBSCRIPTION_ID_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.id = ? and qd.username = ?";

    /**
     * Retrieves subscription metrics records associated with a specific subscription ID and username from the database.
     *
     * @param username       The username associated with the subscription metrics records.
     * @param subscriptionId The ID of the subscription to filter the records by.
     * @return A list of SubscriptionMetrics objects containing the subscription metrics data for the specified subscription and username.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<SubscriptionMetrics> findBySubscriptionId(String username, Long subscriptionId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_SUBSCRIPTION_ID_SQL, new Object[] { subscriptionId, username }, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findBySubscriptionId", e.getMessage(), username, subscriptionId);
        }
    }

    private static final String FIND_BY_USERNAME_SQL = "select * from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.username = ?";

    /**
     * Retrieves subscription metrics records associated with a specific username from the database.
     *
     * @param username The username associated with the subscription metrics records.
     * @return A list of SubscriptionMetrics objects containing the subscription metrics data for the specified username.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<SubscriptionMetrics> findByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USERNAME_SQL, new Object[]{username}, SUBSCRIPTION_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUsername", e.getMessage(), username);
        }
    }

    private static final String FIND_LATEST_BY_USERNAME_SQL = "select qd.queue_id,max(qm.import_timestamp) as max_import_timestamp from subscription_metrics qm join subscription_definitions qd on qd.id = qm.subscription_id where qd.username = ? group by qd.queue_id";

    /**
     * Retrieves the latest import timestamps for subscriptions associated with a specific username from the database.
     *
     * @param username The username associated with the subscriptions.
     * @return A map where the keys are queue IDs, and the values are the latest import timestamps for each queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final Map<Long, Timestamp> findLatestByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_LATEST_BY_USERNAME_SQL, new Object[]{username}, rs -> {
                Map<Long, Timestamp> map = new HashMap<>(512);
                while (rs.next()) {
                    Long feedId = rs.getLong("queue_id");
                    Timestamp maxImportTimestamp = rs.getTimestamp("max_import_timestamp");
                    map.put(feedId, maxImportTimestamp);
                }
                return map;
            });
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findLatestByUsername", e.getMessage(), username);
        }
    }

    //
    //
    //

    private static final String PURGE_ORPHANED = "delete from subscription_metrics where id in (select qm.id from subscription_metrics qm left join subscription_definitions qd on qd.id = qm.subscription_id where qd.id is null)";

    /**
     * Purges orphaned subscription metrics records from the database.
     *
     * @return The number of rows deleted as a result of the purge operation.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final int purgeOrphaned() throws DataAccessException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(PURGE_ORPHANED);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "purgeOrphaned", e.getMessage());
        }

        return rowsUpdated;
    }

    @Override
    public final String toString() {
        return "SubscriptionMetricsDao{" +
                "jdbcTemplate=" + jdbcTemplate +
                ", SUBSCRIPTION_METRICS_ROW_MAPPER=" + SUBSCRIPTION_METRICS_ROW_MAPPER +
                '}';
    }
}
