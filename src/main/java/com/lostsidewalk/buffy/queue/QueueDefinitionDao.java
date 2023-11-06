package com.lostsidewalk.buffy.queue;

import com.google.gson.Gson;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataConflictException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.queue.QueueDefinition.QueueStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static com.lostsidewalk.buffy.queue.QueueDefinition.computeImageHash;
import static java.util.Optional.ofNullable;

/**
 * Data access object for managing queue definitions in the application.
 */
@SuppressWarnings({"deprecation", "OverlyBroadCatchBlock"})
@Slf4j
@Component
public class QueueDefinitionDao {

    private static final Gson GSON = new Gson();
    private static final Pattern DIGITS_PATTERN = Pattern.compile("\\D");

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Default constructor; initializes the object.
     */
    QueueDefinitionDao() {
    }

    private static final String REQUIRES_AUTHENTICATION_BY_TRANSPORT_IDENT_SQL = "select is_authenticated from queue_definitions where transport_ident = ?";

    /**
     * Checks to see if the queue definition with the given transport identifier requires authentication.
     *
     * @param transportIdent The transport identifier of the queue to check.
     * @return True if the queue definition with the provided transport identifier requires authentication.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final Boolean requiresAuthentication(String transportIdent) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(REQUIRES_AUTHENTICATION_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "requiresAuthentication", e.getMessage(), transportIdent);
        }
    }

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from queue_definitions where id = '%s' and is_deleted is false)";

    /**
     * Checks to see if a queue definition with the given exists in the database.
     *
     * @param id The Id to check for.
     * @return True if a queue definition with the provided Id exists in the database.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final Boolean checkExists(String id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_EXISTS_BY_ID_SQL_TEMPLATE, id);
            return jdbcTemplate.queryForObject(sql, null, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
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
                    "is_authenticated, " +
                    "created " +
                    ") values " +
                    "(?,?,?,?,?,?,?,cast(? as json),?,?,?,?,?,?,current_timestamp)";

    /**
     * Adds a new queue definition to the database.
     *
     * @param queueDefinition The queue definition to be added to the database.
     * @return The generated ID of the newly added queue definition.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a data conflict or duplication is encountered during the insertion.
     */
    @SuppressWarnings("unused")
    public final Long add(QueueDefinition queueDefinition) throws DataAccessException, DataUpdateException, DataConflictException {
        int rowsUpdated;
        KeyHolder keyHolder;
        try {
            keyHolder = new GeneratedKeyHolder();
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_queue_definitions_SQL, new String[]{"id"});
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
        } catch (DuplicateKeyException e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataConflictException(getClass().getSimpleName(), "add", e.getMessage(), queueDefinition);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queueDefinition);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queueDefinition);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    private final RowMapper<QueueDefinition> QUEUE_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
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
        if (null != exportConfigObj) {
            exportConfig = exportConfigObj.getValue();
        }
        String copyright = rs.getString("copyright");
        String language = rs.getString("language");
        String queueImgSrc = rs.getString("queue_img_src");
        String queueImgTransportIdent = rs.getString("queue_img_transport_ident");
        Timestamp lastDeployedTimestamp = rs.getTimestamp("last_deployed_timestamp");
        Boolean isAuthenticated = rs.getBoolean("is_authenticated");
        Timestamp created = rs.getTimestamp("created");
        Timestamp lastModified = rs.getTimestamp("last_modified");

        QueueDefinition queueDefinition = QueueDefinition.from(
                queueIdent,
                queueTitle,
                queueDesc,
                queueFeedGenerator,
                transportIdent,
                username,
                QueueStatus.valueOf(queueStatus),
                exportConfig,
                copyright,
                language,
                queueImgSrc,
                queueImgTransportIdent,
                lastDeployedTimestamp,
                isAuthenticated,
                created,
                lastModified
        );
        queueDefinition.setId(id);

        return queueDefinition;
    };

    private static final String MARK_QUEUE_AS_DELETED_BY_ID_SQL = "update queue_definitions set is_deleted = true, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Deletes a queue entry associated with the specified username and identifier.
     *
     * @param username The username associated with the queue entry.
     * @param id The identifier of the queue entry to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the deletion operation fails or no rows were affected.
     */
    @SuppressWarnings("unused")
    public final void deleteById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_QUEUE_AS_DELETED_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from queue_definitions where is_deleted is false";

    /**
     * Retrieves all non-deleted QueueDefinition objects from the database.
     *
     * @return A list of QueueDefinition objects.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<QueueDefinition> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, QUEUE_DEFINITION_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER = "select * from queue_definitions where username = ? and is_deleted is false";

    /**
     * Retrieves all non-deleted QueueDefinition objects associated with a specific user.
     *
     * @param username The username for which to retrieve queue definitions.
     * @return A list of QueueDefinition objects found for the given username, or null if none are found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<QueueDefinition> findByUser(String username) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_USER, new Object[]{username}, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from queue_definitions where username = ? and id = ? and is_deleted is false";

    /**
     * Retrieves a specific non-deleted QueueDefinition object by its identifier and associated user.
     *
     * @param username The username associated with the queue definition.
     * @param id The identifier of the queue definition to retrieve.
     * @return A QueueDefinition object if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final QueueDefinition findByQueueId(String username, Long id) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { username, id }, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByQueueId", e.getMessage(), username, id);
        }
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select * from queue_definitions where transport_ident = ? and is_deleted is false";

    /**
     * Retrieves a specific non-deleted QueueDefinition object by its associated transport identifier.
     *
     * @param transportIdent The transport identifier of the queue definition to retrieve.
     * @return A QueueDefinition object if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final QueueDefinition findByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            List<QueueDefinition> results = jdbcTemplate.query(FIND_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, QUEUE_DEFINITION_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    private static final String FIND_TRANSPORT_IDENT_SQL = "select transport_ident from queue_definitions where username = ? and queue_ident = ?";

    /**
     * Resolves and retrieves the transport identifier for a specific username and queue identifier.
     *
     * @param username The username associated with the transport identifier.
     * @param queueIdent The identifier of the queue.
     * @return The resolved transport identifier as a string, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final String resolveTransportIdent(String username, String queueIdent) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_TRANSPORT_IDENT_SQL, new Object[] { username, queueIdent }, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "resolveTransportIdent", e.getMessage(), username, queueIdent);
        }
    }

    private static final String UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL = "update queue_definitions set last_deployed_timestamp = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the last deployed timestamp for a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param queueId The identifier of the queue.
     * @param lastDeployed The timestamp indicating the last deployment time.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateLastDeployed(String username, Long queueId, Date lastDeployed) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_LAST_DEPLOYED_TIMESTAMP_SQL, toTimestamp(lastDeployed), queueId, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateLastDeployed", e.getMessage(), username, queueId, lastDeployed);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateLastDeployed", username, queueId, lastDeployed);
        }
    }

    private static final String CHECK_DEPLOYED_BY_ID_SQL_TEMPLATE = "select (last_deployed_timestamp is not null) from queue_definitions where id = %s and username = ? and is_deleted is false";

    /**
     * Checks if a specific queue is deployed by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue to check for deployment.
     * @return True if the queue is deployed, false otherwise.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final Boolean checkDeployed(String username, long id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_DEPLOYED_BY_ID_SQL_TEMPLATE, DIGITS_PATTERN.matcher(String.valueOf(id)).replaceAll(""));
            return jdbcTemplate.queryForObject(sql, new Object[]{username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "checkPublished", e.getMessage(), username, id);
        }
    }

    private static final String CLEAR_LAST_DEPLOYED_BY_ID_SQL = "update queue_definitions set last_deployed_timestamp = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the last deployed timestamp for a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue to clear the last deployed timestamp for.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearLastDeployed(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_LAST_DEPLOYED_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearLastDeployed", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearLastDeployed", username, id);
        }
    }

    private static final String UPDATE_queue_status_BY_ID = "update queue_definitions set queue_status = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the status of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueStatus The new QueueStatus to set for the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueStatus(String username, long id, QueueStatus queueStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_queue_status_BY_ID, queueStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueStatus", e.getMessage(), username, id, queueStatus);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueStatus", username, id, queueStatus);
        }
    }

    private static final String UPDATE_BY_ID_SQL_TEMPLATE = "update queue_definitions set %s where id = ? and username = ?";

    /**
     * Updates various attributes of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueIdent The new queue identifier.
     * @param description The new queue description.
     * @param title The new queue title.
     * @param generator The new queue feed generator.
     * @param exportConfig The new export configuration as a serializable object.
     * @param copyright The new queue copyright.
     * @param language The new queue language.
     * @param queueImgSrc The new queue image source.
     * @param isAuthenticated The new authentication status for the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a data conflict or duplication is encountered during the update.
     */
    @SuppressWarnings("unused")
    public final void updateQueue(String username, Long id, String queueIdent, String description, String title, String generator,
                                  Serializable exportConfig, String copyright, String language, String queueImgSrc, Boolean isAuthenticated)
            throws DataAccessException, DataUpdateException, DataConflictException {
        String queueImgTransportIdent = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            queueImgTransportIdent = computeImageHash(md, queueImgSrc);
        } catch (NoSuchAlgorithmException ignored) {
            // ignored
        }

        StringBuilder sqlBuilder = new StringBuilder(512);
        Collection<Object> sqlParams = new ArrayList<>(12);
        if (null != queueIdent) {
            sqlBuilder.append("queue_ident = ?, ");
            sqlParams.add(queueIdent);
        }
        if (null != description) {
            sqlBuilder.append("queue_desc = ?, ");
            sqlParams.add(description);
        }
        if (null != title) {
            sqlBuilder.append("queue_title = ?, ");
            sqlParams.add(title);
        }
        if (null != generator) {
            sqlBuilder.append("queue_feed_generator = ?, ");
            sqlParams.add(generator);
        }
        if (null != exportConfig) {
            sqlBuilder.append("export_config = ?::json, ");
            sqlParams.add(exportConfig);
        }
        if (null != copyright) {
            sqlBuilder.append("copyright = ?, ");
            sqlParams.add(copyright);
        }
        if (null != language) {
            sqlBuilder.append("language = ?, ");
            sqlParams.add(language);
        }
        if (null != queueImgSrc) {
            sqlBuilder.append("queue_img_src = ?, ");
            sqlParams.add(queueImgSrc);
        }
        if (null != queueImgTransportIdent) {
            sqlBuilder.append("queue_img_transport_ident = ?, ");
            sqlParams.add(queueImgTransportIdent);
        }
        if (null != isAuthenticated) {
            sqlBuilder.append("is_authenticated = ? ");
            sqlParams.add(isAuthenticated);
        }

        if (sqlParams.isEmpty()) {
            return; // no updates required, return quietly
        }

        sqlBuilder.append(", last_modified = current_timestamp ");
        sqlParams.add(id);
        sqlParams.add(username);

        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(String.format(UPDATE_BY_ID_SQL_TEMPLATE, sqlBuilder), sqlParams.toArray());
        } catch (DuplicateKeyException e) {
            throw new DataConflictException(getClass().getSimpleName(), "updateQueue", e.getMessage(),
                    username, queueIdent, description, title, generator, exportConfig, copyright, language, queueImgSrc);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueue", e.getMessage(),
                    username, queueIdent, description, title, generator, exportConfig, copyright, language, queueImgSrc);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueue",
                    username, queueIdent, description, title, generator, exportConfig, copyright, language, queueImgSrc);
        }
    }
    
    //
    //
    //
    
    private static final String UPDATE_QUEUE_IDENT_BY_ID = "update queue_definitions set queue_ident = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the identifier of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueIdent The new queue identifier.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a data conflict or duplication is encountered during the update.
     */
    @SuppressWarnings("unused")
    public final void updateQueueIdent(String username, Long id, String queueIdent) throws DataAccessException, DataUpdateException, DataConflictException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_IDENT_BY_ID, queueIdent, id, username);
        } catch (DuplicateKeyException e) {
            throw new DataConflictException(getClass().getSimpleName(), "updateQueueIdent", e.getMessage(), username, id, queueIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueIdent", e.getMessage(), username, id, queueIdent);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueIdent", username, id, queueIdent);
        }
    }

    private static final String UPDATE_QUEUE_TITLE_BY_ID = "update queue_definitions set queue_title = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the title of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueTitle The new queue title.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueTitle(String username, Long id, String queueTitle) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_TITLE_BY_ID, queueTitle, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueTitle", e.getMessage(), username, id, queueTitle);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueTitle", username, id, queueTitle);
        }
    }

    private static final String UPDATE_QUEUE_DESCRIPTION_BY_ID = "update queue_definitions set queue_desc = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the description of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueDescription The new queue description.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueDescription(String username, Long id, String queueDescription) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_DESCRIPTION_BY_ID, queueDescription, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueDescription", e.getMessage(), username, id, queueDescription);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueDescription", username, id, queueDescription);
        }
    }

    private static final String UPDATE_QUEUE_GENERATOR_BY_ID = "update queue_definitions set queue_feed_generator = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the generator of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueGenerator The new queue generator.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueGenerator(String username, Long id, String queueGenerator) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_GENERATOR_BY_ID, queueGenerator, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueGenerator", e.getMessage(), username, id, queueGenerator);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueGenerator", username, id, queueGenerator);
        }
    }

    private static final String UPDATE_QUEUE_COPYRIGHT_BY_ID = "update queue_definitions set copyright = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the copyright of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param copyright The new queue copyright string.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateCopyright(String username, Long id, String copyright) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_COPYRIGHT_BY_ID, copyright, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateCopyright", e.getMessage(), username, id, copyright);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateCopyright", username, id, copyright);
        }
    }

    private static final String UPDATE_LANGUAGE_BY_ID = "update queue_definitions set language = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the language of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param language The new queue language.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateLanguage(String username, Long id, String language) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_LANGUAGE_BY_ID, language, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateLanguage", e.getMessage(), username, id, language);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateLanguage", username, id, language);
        }
    }

    private static final String UPDATE_QUEUE_AUTH_REQUIREMENT_ID = "update queue_definitions set is_authenticated = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the authentication requirement of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param isRequired The new queue authentication requirement value.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueAuthenticationRequirement(String username, Long id, Boolean isRequired) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_AUTH_REQUIREMENT_ID, isRequired, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueAuthenticationRequirement", e.getMessage(), username, id, isRequired);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueAuthenticationRequirement", username, id, isRequired);
        }
    }

    private static final String UPDATE_QUEUE_IMAGE_SOURCE_BY_ID = "update queue_definitions set queue_img_src = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the image source of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param queueImageSource The new queue image source.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateQueueImageSource(String username, Long id, String queueImageSource) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_IMAGE_SOURCE_BY_ID, queueImageSource, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueImageSource", e.getMessage(), username, id, queueImageSource);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueImageSource", username, id, queueImageSource);
        }
    }

    private static final String UPDATE_QUEUE_EXPORT_CONFIG_BY_ID = "update queue_definitions set export_config = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the export configuration of a specific queue by its username and identifier.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @param exportConfig The new export configuration.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void updateExportConfig(String username, Long id, Serializable exportConfig) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_QUEUE_EXPORT_CONFIG_BY_ID, exportConfig.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueExportConfig", e.getMessage(), username, id, exportConfig.toString());
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueExportConfig", username, id, exportConfig.toString());
        }
    }

    private static final String CLEAR_QUEUE_TITLE_BY_ID_SQL = "update queue_definitions set queue_title = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the title of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearQueueTitle(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_TITLE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueTitle", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueTitle", username, id);
        }
    }

    private static final String CLEAR_QUEUE_DESCRIPTION_BY_ID_SQL = "update queue_definitions set queue_desc = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the description of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearQueueDescription(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_DESCRIPTION_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueDescription", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueDescription", username, id);
        }
    }

    private static final String CLEAR_QUEUE_GENERATOR_BY_ID_SQL = "update queue_definitions set queue_feed_generator = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the generator of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearQueueGenerator(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_GENERATOR_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueGenerator", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueGenerator", username, id);
        }
    }

    private static final String CLEAR_QUEUE_COPYRIGHT_BY_ID_SQL = "update queue_definitions set copyright = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the copyright string of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearQueueCopyright(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_COPYRIGHT_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueCopyright", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueCopyright", username, id);
        }
    }

    private static final String CLEAR_QUEUE_IMG_SRC_BY_ID_SQL = "update queue_definitions set queue_img_src = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the image source of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearQueueImageSource(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_IMG_SRC_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueImageSource", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueImageSource", username, id);
        }
    }

    private static final String CLEAR_QUEUE_EXPORT_CONFIG_BY_ID_SQL = "update queue_definitions set export_config = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the export configuration of a queue identified by its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param id The identifier of the queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public final void clearExportConfig(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_QUEUE_EXPORT_CONFIG_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearQueueImageSource", e.getMessage(), username, id);
        }
        if (!(0 < rowsUpdated)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearQueueImageSource", username, id);
        }
    }

    //
    //
    //

    private static final String FIND_ID_SQL = "select id from queue_definitions where username = ? and queue_ident = ?";

    /**
     * Resolves the ID of a queue based on its identifier for a specific user.
     *
     * @param username   The username associated with the queue.
     * @param queueIdent The identifier of the queue.
     * @return The ID of the queue, or null if no matching queue is found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final Long resolveId(String username, String queueIdent) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_ID_SQL, new Object[] { username, queueIdent }, Long.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "resolveId", e.getMessage(), username, queueIdent);
        }
    }

    private static final String FIND_IDENT_SQL = "select queue_ident from queue_definitions where username = ? and id = ?";

    /**
     * Resolves the identifier of a queue based on its ID for a specific user.
     *
     * @param username The username associated with the queue.
     * @param queueId  The ID of the queue.
     * @return The identifier of the queue, or null if no matching queue is found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final String resolveIdent(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_IDENT_SQL, new Object[] { username, queueId }, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "resolveIdent", e.getMessage(), username, queueId);
        }
    }

    //
    //
    //

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date date) {
        Instant instant = null != date ? OffsetDateTime.from(date.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return null != instant ? Timestamp.from(instant) : null;
    }

    //
    //
    //

    private static final String PURGE_DELETED_SQL = "delete from queue_definitions where is_deleted is true";

    /**
     * Purges (permanently deletes) all queue definitions that are marked as deleted in the database.
     *
     * @return The number of rows deleted as a result of the operation.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final int purgeDeleted() throws DataAccessException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(PURGE_DELETED_SQL);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "purgeDeleted", e.getMessage());
        }

        return rowsUpdated;
    }

    @Override
    public final String toString() {
        return "QueueDefinitionDao{" +
                "jdbcTemplate=" + jdbcTemplate +
                ", QUEUE_DEFINITION_ROW_MAPPER=" + QUEUE_DEFINITION_ROW_MAPPER +
                '}';
    }
}
