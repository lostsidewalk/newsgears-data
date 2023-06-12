package com.lostsidewalk.buffy.queue;

import com.google.gson.Gson;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.queue.QueueDefinition.QueueStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.lostsidewalk.buffy.queue.QueueDefinition.computeImageHash;
import static java.util.Optional.ofNullable;

@Slf4j
@Component
public class QueueDefinitionDao {

    private static final Gson GSON = new Gson();

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String REQUIRES_AUTHENTICATION_BY_TRANSPORT_IDENT_SQL = "select is_authenticated from queue_definitions where transport_ident = ?";

    @SuppressWarnings("unused")
    public Boolean requiresAuthentication(String transportIdent) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(REQUIRES_AUTHENTICATION_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "requiresAuthentication", e.getMessage(), transportIdent);
        }
    }

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from queue_definitions where id = '%s' and is_deleted is false)";

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

    private static final String INSERT_queue_definitions_SQL =
            "insert into queue_definitions (" +
                    "queue_ident," +
                    "queue_title," +
                    "queue_desc," +
                    "queue_feed_generator," +
                    "transport_ident," +
                    "username," +
                    "queue_status," +
                    "export_config," +
                    "copyright," +
                    "language, " +
                    "queue_img_src, " +
                    "queue_img_transport_ident, " +
                    "last_deployed_timestamp, " +
                    "is_authenticated " +
                    ") values " +
                    "(?,?,?,?,?,?,?,cast(? as json),?,?,?,?,?,?)";

    @SuppressWarnings("unused")
    public Long add(QueueDefinition queueDefinition) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        KeyHolder keyHolder;
        try {
            keyHolder = new GeneratedKeyHolder();
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_queue_definitions_SQL, new String[] { "id" });
                        ps.setString(1, queueDefinition.getIdent());
                        ps.setString(2, queueDefinition.getTitle());
                        ps.setString(3, queueDefinition.getDescription());
                        ps.setString(4, queueDefinition.getGenerator());
                        ps.setString(5, queueDefinition.getTransportIdent());
                        ps.setString(6, queueDefinition.getUsername());
                        ps.setString(7, queueDefinition.getQueueStatus().toString());
                        ps.setString(8, ofNullable(queueDefinition.getExportConfig()).map(GSON::toJson).orElse(null));
                        ps.setString(9, queueDefinition.getCopyright());
                        ps.setString(10, queueDefinition.getLanguage());
                        ps.setString(11, queueDefinition.getQueueImgSrc());
                        ps.setString(12, queueDefinition.getQueueImgTransportIdent());
                        ps.setTimestamp(13, toTimestamp(queueDefinition.getLastDeployed()));
                        ps.setBoolean(14, queueDefinition.getIsAuthenticated());

                        return ps;
                    }, keyHolder);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queueDefinition);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queueDefinition);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    final RowMapper<QueueDefinition> QUEUE_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String queueIdent = rs.getString("queue_ident");
        String queueTitle = rs.getString("queue_title");
        String queueDesc = rs.getString("queue_desc");
        String queueFeedGenerator = rs.getString("queue_feed_generator");
        String transportIdent = rs.getString("transport_ident");
        String username = rs.getString("username");
        String queueStatus = rs.getString("queue_status");
        String exportConfig = null;
        PGobject exportConfigObj = (PGobject) rs.getObject("export_config");
        if (exportConfigObj != null) {
            exportConfig = exportConfigObj.getValue();
        }
        String queueFeedCopyright = rs.getString("copyright");
        String queueFeedLanguage = rs.getString("language");
        String queueImgSrc = rs.getString("queue_img_src");
        String queueImgTransportIdent = rs.getString("queue_img_transport_ident");
        Timestamp lastDeployedTimestamp = rs.getTimestamp("last_deployed_timestamp");
        Boolean isAuthenticated = rs.getBoolean("is_authenticated");

        QueueDefinition f = QueueDefinition.from(
                queueIdent,
                queueTitle,
                queueDesc,
                queueFeedGenerator,
                transportIdent,
                username,
                QueueStatus.valueOf(queueStatus),
                exportConfig,
                queueFeedCopyright,
                queueFeedLanguage,
                queueImgSrc,
                queueImgTransportIdent,
                lastDeployedTimestamp,
                isAuthenticated
        );
        f.setId(id);

        return f;
    };

    private static final String MARK_QUEUE_AS_DELETED_BY_ID_SQL = "update queue_definitions set is_deleted = true where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void deleteById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_QUEUE_AS_DELETED_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from queue_definitions where is_deleted is false";

    @SuppressWarnings("unused")
    public List<QueueDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, QUEUE_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER = "select * from queue_definitions where username = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public List<QueueDefinition> findByUser(String username) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_USER, new Object[]{username}, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from queue_definitions where username = ? and id = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public QueueDefinition findByQueueId(String username, Long id) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { username, id }, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, id);
        }
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select * from queue_definitions where transport_ident = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public QueueDefinition findByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    private static final String UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL = "update queue_definitions set last_deployed_timestamp = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateLastDeployed(String username, Long queueId, Date lastDeployed) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL, toTimestamp(lastDeployed), queueId, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateLastDeployed", e.getMessage(), username, queueId, lastDeployed);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateLastDeployed", username, queueId, lastDeployed);
        }
    }

    private static final String CHECK_DEPLOYED_BY_ID_SQL_TEMPLATE = "select (last_deployed_timestamp is not null) from queue_definitions where id = %s and username = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public Boolean checkDeployed(String username, long id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_DEPLOYED_BY_ID_SQL_TEMPLATE, String.valueOf(id).replaceAll("\\D", ""));
            return jdbcTemplate.queryForObject(sql, new Object[]{username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkPublished", e.getMessage(), username, id);
        }
    }

    private static final String CLEAR_LAST_DEPLOYED_BY_ID_SQL = "update queue_definitions set last_deployed_timestamp = null where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void clearLastDeployed(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_LAST_DEPLOYED_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "clearLastDeployed", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearLastDeployed", username, id);
        }
    }

    private static final String UPDATE_queue_status_BY_ID = "update queue_definitions set queue_status = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateQueueStatus(String username, long id, QueueStatus queueStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_queue_status_BY_ID, queueStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueStatus", e.getMessage(), username, id, queueStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueStatus", username, id, queueStatus);
        }
    }

    private static final String UPDATE_BY_ID_SQL_TEMPLATE = "update queue_definitions set %s where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateFeed(String username, Long id, String feedIdent, String description, String title, String generator,
                       Serializable exportConfig, String copyright, String language, String feedImgSrc, Boolean isAuthenticated)
            throws DataAccessException, DataUpdateException {
        String feedImgTransportIdent = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            feedImgTransportIdent = computeImageHash(md, feedImgSrc);
        } catch (NoSuchAlgorithmException ignored) {
            // ignored
        }

        StringBuilder sqlBuilder = new StringBuilder();
        List<Object> sqlParams = new ArrayList<>();
        if (feedIdent != null) {
            sqlBuilder.append("queue_ident = ?, ");
            sqlParams.add(feedIdent);
        }
        if (description != null) {
            sqlBuilder.append("queue_desc = ?, ");
            sqlParams.add(description);
        }
        if (title != null) {
            sqlBuilder.append("queue_title = ?, ");
            sqlParams.add(title);
        }
        if (generator != null) {
            sqlBuilder.append("queue_feed_generator = ?, ");
            sqlParams.add(generator);
        }
        if (exportConfig != null) {
            sqlBuilder.append("export_config = ?::json, ");
            sqlParams.add(exportConfig);
        }
        if (copyright != null) {
            sqlBuilder.append("copyright = ?, ");
            sqlParams.add(copyright);
        }
        if (language != null) {
            sqlBuilder.append("language = ?, ");
            sqlParams.add(language);
        }
        if (feedImgSrc != null) {
            sqlBuilder.append("queue_img_src = ?, ");
            sqlParams.add(feedImgSrc);
        }
        if (feedImgTransportIdent != null) {
            sqlBuilder.append("queue_img_transport_ident = ?, ");
            sqlParams.add(feedImgTransportIdent);
        }
        if (isAuthenticated != null) {
            sqlBuilder.append("is_authenticated = ? ");
            sqlParams.add(isAuthenticated);
        }

        if (sqlParams.isEmpty()) {
            return; // no updates required, return quietly
        }

        sqlParams.add(id);
        sqlParams.add(username);

        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(String.format(UPDATE_BY_ID_SQL_TEMPLATE, sqlBuilder), sqlParams.toArray());
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateFeed", e.getMessage(),
                    username, feedIdent, description, title, generator, exportConfig, copyright, language, feedImgSrc);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateFeed",
                    username, feedIdent, description, title, generator, exportConfig, copyright, language, feedImgSrc);
        }
    }

    //
    //
    //

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date d) {
        Instant i = d != null ? OffsetDateTime.from(d.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return i != null ? Timestamp.from(i) : null;
    }

    //
    //
    //

    private static final String PURGE_DELETED_SQL = "delete from queue_definitions where is_deleted is true";

    @SuppressWarnings("unused")
    public int purgeDeleted() throws DataAccessException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(PURGE_DELETED_SQL);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "purgeDeleted", e.getMessage());
        }

        return rowsUpdated;
    }
}
