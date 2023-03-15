package com.lostsidewalk.buffy.feed;

import com.google.gson.Gson;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.feed.FeedDefinition.FeedStatus;
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
import java.util.Date;
import java.util.List;

import static com.lostsidewalk.buffy.feed.FeedDefinition.computeImageHash;
import static java.util.Optional.ofNullable;

@Slf4j
@Component
public class FeedDefinitionDao {

    private static final Gson GSON = new Gson();

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from feed_definitions where id = '%s' and is_deleted is false)";

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

    private static final String INSERT_FEED_DEFINITIONS_SQL =
            "insert into feed_definitions (" +
                    "feed_ident," +
                    "feed_title," +
                    "feed_desc," +
                    "feed_generator," +
                    "transport_ident," +
                    "username," +
                    "feed_status," +
                    "export_config," +
                    "copyright," +
                    "language, " +
                    "feed_img_src, " +
                    "feed_img_transport_ident, " +
                    "last_deployed_timestamp " +
                    ") values " +
                    "(?,?,?,?,?,?,?,cast(? as json),?,?,?,?,?)";

    @SuppressWarnings("unused")
    public Long add(FeedDefinition feedDefinition) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        KeyHolder keyHolder;
        try {
            keyHolder = new GeneratedKeyHolder();
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_FEED_DEFINITIONS_SQL, new String[] { "id" });
                        ps.setString(1, feedDefinition.getIdent());
                        ps.setString(2, feedDefinition.getTitle());
                        ps.setString(3, feedDefinition.getDescription());
                        ps.setString(4, feedDefinition.getGenerator());
                        ps.setString(5, feedDefinition.getTransportIdent());
                        ps.setString(6, feedDefinition.getUsername());
                        ps.setString(7, feedDefinition.getFeedStatus().toString());
                        ps.setString(8, ofNullable(feedDefinition.getExportConfig()).map(GSON::toJson).orElse(null));
                        ps.setString(9, feedDefinition.getCopyright());
                        ps.setString(10, feedDefinition.getLanguage());
                        ps.setString(11, feedDefinition.getFeedImgSrc());
                        ps.setString(12, feedDefinition.getFeedImgTransportIdent());
                        ps.setTimestamp(13, toTimestamp(feedDefinition.getLastDeployed()));

                        return ps;
                    }, keyHolder);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), feedDefinition);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", feedDefinition);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    final RowMapper<FeedDefinition> FEED_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String feedIdent = rs.getString("feed_ident");
        String feedTitle = rs.getString("feed_title");
        String feedDesc = rs.getString("feed_desc");
        String feedGenerator = rs.getString("feed_generator");
        String transportIdent = rs.getString("transport_ident");
        String username = rs.getString("username");
        String feedStatus = rs.getString("feed_status");
        String exportConfig = null;
        PGobject exportConfigObj = (PGobject) rs.getObject("export_config");
        if (exportConfigObj != null) {
            exportConfig = exportConfigObj.getValue();
        }
        String feedCopyright = rs.getString("copyright");
        String feedLanguage = rs.getString("language");
        String feedImgSrc = rs.getString("feed_img_src");
        String feedImgTransportIdent = rs.getString("feed_img_transport_ident");
        Timestamp lastDeployedTimestamp = rs.getTimestamp("last_deployed_timestamp");

        FeedDefinition f = FeedDefinition.from(
                feedIdent,
                feedTitle,
                feedDesc,
                feedGenerator,
                transportIdent,
                username,
                FeedStatus.valueOf(feedStatus),
                exportConfig,
                feedCopyright,
                feedLanguage,
                feedImgSrc,
                feedImgTransportIdent,
                lastDeployedTimestamp
        );
        f.setId(id);

        return f;
    };

    private static final String MARK_FEED_AS_DELETED_BY_ID_SQL = "update feed_definitions set is_deleted = true where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void deleteById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_FEED_AS_DELETED_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from feed_definitions where is_deleted is false";

    @SuppressWarnings("unused")
    public List<FeedDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, FEED_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER = "select * from feed_definitions where username = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public List<FeedDefinition> findByUser(String username) throws DataAccessException {
        try {
            List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_USER, new Object[]{username}, FEED_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_IDENTS_BY_USER = "select distinct(feed_ident) from feed_definitions where username = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public List<String> findIdentsByUser(String username) throws DataAccessException {
        try {
            List<String> results = jdbcTemplate.query(FIND_IDENTS_BY_USER, new Object[]{username}, (rs, rowNum) -> rs.getString("feed_ident"));
            return results.isEmpty() ? null : results;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findIdentsByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_BY_FEED_ID_SQL = "select * from feed_definitions where username = ? and id = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public FeedDefinition findByFeedId(String username, Long id) throws DataAccessException {
        try {
            List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_FEED_ID_SQL, new Object[] { username, id }, FEED_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedId", e.getMessage(), username, id);
        }
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select * from feed_definitions where transport_ident = ? and is_deleted is false";

    @SuppressWarnings("unused")
    public FeedDefinition findByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, FEED_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    private static final String UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL = "update feed_definitions set last_deployed_timestamp = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateLastDeployed(String username, Long feedId, Date lastDeployed) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL, toTimestamp(lastDeployed), feedId, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateLastDeployed", e.getMessage(), username, feedId, lastDeployed);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateLastDeployed", username, feedId, lastDeployed);
        }
    }

    private static final String CHECK_DEPLOYED_BY_ID_SQL_TEMPLATE = "select (last_deployed_timestamp is not null) from feed_definitions where id = %s and username = ? and is_deleted is false";

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

    private static final String CLEAR_LAST_DEPLOYED_BY_ID_SQL = "update feed_definitions set last_deployed_timestamp = null where id = ? and username = ?";

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

    private static final String UPDATE_FEED_STATUS_BY_ID = "update feed_definitions set feed_status = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateFeedStatus(String username, long id, FeedStatus feedStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_FEED_STATUS_BY_ID, feedStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateFeedStatus", e.getMessage(), username, id, feedStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateFeedStatus", username, id, feedStatus);
        }
    }

    private static final String UPDATE_BY_ID_SQL = "update feed_definitions set " +
            "feed_ident = ?, " +
            "feed_desc = ?, " +
            "feed_title = ?, " +
            "feed_generator = ?, " +
            "export_config = ?::json, " +
            "copyright = ?, " +
            "language = ?, " +
            "feed_img_src = ?, " +
            "feed_img_transport_ident = ? " +
        "where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateFeed(String username, Long id, String feedIdent, String description, String title, String generator,
                       Serializable exportConfig, String copyright, String language, String feedImgSrc) throws DataAccessException, DataUpdateException {
        String feedImgTransportIdent = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            feedImgTransportIdent = computeImageHash(md, feedImgSrc);
        } catch (NoSuchAlgorithmException ignored) {
            // ignored
        }
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_BY_ID_SQL,
                    feedIdent,
                    description,
                    title,
                    generator,
                    exportConfig,
                    copyright,
                    language,
                    feedImgSrc,
                    feedImgTransportIdent,
                    id,
                    username
            );
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
}
