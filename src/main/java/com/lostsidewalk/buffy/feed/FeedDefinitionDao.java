package com.lostsidewalk.buffy.feed;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import static com.lostsidewalk.buffy.feed.FeedDefinition.computeImageHash;

@Slf4j
@Component
public class FeedDefinitionDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from feed_definitions where id = '%s')";

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
                    "is_active," +
                    "export_config," +
                    "copyright," +
                    "language, " +
                    "feed_img_src, " +
                    "feed_img_transport_ident, " +
                    "last_deployed_timestamp " +
                    ") values " +
                    "(?,?,?,?,?,?,?,cast(? as json),?,?,?,?,?)";

    @SuppressWarnings("unused")
    public void add(FeedDefinition feedDefinition) throws DataAccessException {
        try {
            int rowsUpdated = jdbcTemplate.update(INSERT_FEED_DEFINITIONS_SQL,
                    feedDefinition.getIdent(),
                    feedDefinition.getTitle(),
                    feedDefinition.getDescription(),
                    feedDefinition.getGenerator(),
                    feedDefinition.getTransportIdent(),
                    feedDefinition.getUsername(),
                    feedDefinition.isActive(),
                    feedDefinition.getExportConfig(),
                    feedDefinition.getCopyright(),
                    feedDefinition.getLanguage(),
                    feedDefinition.getFeedImgSrc(),
                    feedDefinition.getFeedImgTransportIdent(),
                    toTimestamp(feedDefinition.getLastDeployed())
            );
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "add", feedDefinition);
            }
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), feedDefinition);
        }
    }

    final RowMapper<FeedDefinition> FEED_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String feedIdent = rs.getString("feed_ident");
        String feedTitle = rs.getString("feed_title");
        String feedDesc = rs.getString("feed_desc");
        String feedGenerator = rs.getString("feed_generator");
        String transportIdent = rs.getString("transport_ident");
        String username = rs.getString("username");
        boolean isActive = rs.getBoolean("is_active");
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

        FeedDefinition f = new FeedDefinition(
                feedIdent,
                feedTitle,
                feedDesc,
                feedGenerator,
                transportIdent,
                username,
                isActive,
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

    private static final String DELETE_BY_ID_SQL = "delete from feed_definitions where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void deleteById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from feed_definitions";

    @SuppressWarnings("unused")
    public List<FeedDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, FEED_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER = "select * from feed_definitions where username = ?";

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

    private static final String FIND_IDENTS_BY_USER = "select distinct(feed_ident) from feed_definitions where username = ?";

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

    private static final String FIND_BY_FEED_IDENT_SQL = "select * from feed_definitions where username = ? and feed_ident = ?";

    @SuppressWarnings("unused")
    public FeedDefinition findByFeedIdent(String username, String feedIdent) throws DataAccessException {
        try {
            List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_FEED_IDENT_SQL, new Object[] { username, feedIdent }, FEED_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByFeedIdent", e.getMessage(), username, feedIdent);
        }
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select * from feed_definitions where transport_ident = ?";

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

    private static final String TOGGLE_ACTIVE_BY_ID_SQL = "update feed_definitions set is_active = not is_active where id = ? and username = ?";

    private static final String CHECK_ACTIVE_BY_ID_SQL = "select is_active from feed_definitions where id = ? and username = ?";

    @SuppressWarnings("unused")
    public Boolean toggleActiveById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(TOGGLE_ACTIVE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "toggleActiveById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "toggleActiveById", username, id);
        }
        try {
            return jdbcTemplate.queryForObject(CHECK_ACTIVE_BY_ID_SQL, new Object[]{id, username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "toggleActiveById", e.getMessage(), username, id);
        }
    }

    private static final String MARK_ACTIVE_BY_ID_SQL = "update feed_definitions set is_active = true where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void markActiveById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_ACTIVE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "markActiveById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "markActiveById", username, id);
        }
    }

    private static final String UPDATE_LAST_DEPLOYED_TIMESTAMP_BY_IDENT_SQL = "update feed_definitions set last_deployed_timestamp = ? where feed_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateLastDeployed(String username, String feedIdent, Date lastDeployed) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_LAST_DEPLOYED_TIMESTAMP_BY_IDENT_SQL, toTimestamp(lastDeployed), feedIdent, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateLastDeployed", e.getMessage(), username, feedIdent, lastDeployed);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateLastDeployed", username, feedIdent, lastDeployed);
        }
    }

    private static final String UPDATE_BY_IDENT_SQL = "update feed_definitions set " +
            "feed_desc = ?, " +
            "feed_title = ?, " +
            "feed_generator = ?, " +
            "export_config = ?::json, " +
            "copyright = ?, " +
            "language = ?, " +
            "feed_img_src = ?, " +
            "feed_img_transport_ident = ? " +
        "where feed_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateFeed(String username, String feedIdent, String description, String title, String generator,
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
            rowsUpdated = jdbcTemplate.update(UPDATE_BY_IDENT_SQL,
                    description,
                    title,
                    generator,
                    exportConfig,
                    copyright,
                    language,
                    feedImgSrc,
                    feedImgTransportIdent,
                    feedIdent,
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
