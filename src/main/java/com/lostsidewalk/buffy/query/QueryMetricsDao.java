package com.lostsidewalk.buffy.query;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

import static java.util.Optional.ofNullable;

@Slf4j
@Component
public class QueryMetricsDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String INSERT_QUERY_METRICS_SQL =
            "insert into query_metrics (" +
                    "query_id," +
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
    public void add(QueryMetrics queryMetrics) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(INSERT_QUERY_METRICS_SQL,
                    queryMetrics.getQueryId(),
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
    final RowMapper<QueryMetrics> QUERY_METRICS_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long queryId = rs.getLong("query_id");
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

        QueryMetrics q = QueryMetrics.from(
                queryId,
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
        q.setErrorType(ofNullable(errorType).map(e -> QueryMetrics.QueryExceptionType.valueOf(errorType)).orElse(null)); // TODO: safety
        q.setErrorDetail(errorDetail);

        return q;
    };

    private static final String FIND_ALL_SQL = "select * from query_metrics";

    @SuppressWarnings("unused")
    public List<QueryMetrics> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, QUERY_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_FEED_ID_SQL = "select * from query_metrics qm join query_definitions qd on qd.id = qm.query_id where qd.feed_id = ? and qd.username = ?";

    @SuppressWarnings("unused")
    public List<QueryMetrics> findByFeedId(String username, Long feedId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_FEED_ID_SQL, new Object[] { feedId, username }, QUERY_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, feedId);
        }
    }

    private static final String FIND_BY_QUERY_ID_SQL = "select * from query_metrics qm join query_definitions qd on qd.id = qm.query_id where qd.id = ? and qd.username = ?";

    @SuppressWarnings("unused")
    public List<QueryMetrics> findByQueryId(String username, Long queryId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUERY_ID_SQL, new Object[] { queryId, username }, QUERY_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByQueryId", e.getMessage(), username, queryId);
        }
    }

    private static final String FIND_BY_USERNAME_SQL = "select * from query_metrics qm join query_definitions qd on qd.id = qm.query_id where qd.username = ?";

    @SuppressWarnings("unused")
    public List<QueryMetrics> findByUsername(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USERNAME_SQL, new Object[]{username}, QUERY_METRICS_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUsername", e.getMessage(), username);
        }
    }

    //
    //
    //

    private static final String PURGE_ORPHANED = "delete from query_metrics where id in (select qm.id from query_metrics qm left join query_definitions qd on qd.id = qm.query_id where qd.id is null)";

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
