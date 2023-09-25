package com.lostsidewalk.buffy.post;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataConflictException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.post.StagingPost.PostPubStatus;
import com.lostsidewalk.buffy.post.StagingPost.PostReadStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;

import static com.lostsidewalk.buffy.post.StagingPost.computeThumbnailHash;
import static java.sql.Types.INTEGER;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Data access object for managing staging posts in the application.
 */
@SuppressWarnings("deprecation")
@Slf4j
@Component
public class StagingPostDao {

    private static final Gson GSON = new Gson();

    private static final Type LIST_POST_URL_TYPE = new TypeToken<List<PostUrl>>() {}.getType();

    private static final Type LIST_POST_PERSON_TYPE = new TypeToken<List<PostPerson>>() {}.getType();

    private static final Type LIST_POST_ENCLOSURE_TYPE = new TypeToken<List<PostEnclosure>>() {}.getType();

    private static final Type LIST_CONTENT_OBJECT_TYPE = new TypeToken<List<ContentObject>>() {}.getType();

    private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() {}.getType();

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_HASH_SQL_TEMPLATE = "select exists(select id from staging_posts where post_hash = '%s')";

    /**
     * Checks whether a staging post with the given hash exists in the database.
     *
     * @param stagingPostHash The hash of the staging post to check for existence.
     * @return {@code true} if a staging post with the given hash exists; {@code false} otherwise.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    Boolean checkExists(String stagingPostHash) throws DataAccessException {
        try {
            String sql = String.format(CHECK_EXISTS_BY_HASH_SQL_TEMPLATE, stagingPostHash);
            return jdbcTemplate.queryForObject(sql, null, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), stagingPostHash);
        }
    }

    private static final String INSERT_STAGING_POST_SQL =
            "insert into staging_posts (" +
                    "post_hash," +
                    "post_title," +
                    "post_desc," +
                    "post_contents," +
                    "post_media," +
                    "post_itunes," +
                    "post_url," +
                    "post_urls," +
                    "post_img_url," +
                    "post_img_transport_ident," +
                    "importer_id," +
                    "importer_desc," +
                    "subscription_id," +
                    "queue_id," +
                    "import_timestamp," +
                    "post_read_status," +
                    "post_pub_status," +
                    "username," +
                    "post_comment," +
                    "post_rights," +
                    "contributors," +
                    "authors," +
                    "post_categories," +
                    "publish_timestamp," +
                    "expiration_timestamp," +
                    "enclosures," +
                    "last_updated_timestamp," +
                    "created" +
                    ") values (" +
                    "?," + // post_hash
                    "cast(? as json)," + // post_title
                    "cast(? as json)," + // post_desc
                    "cast(? as json)," + // post_contents
                    "cast(? as json)," + // post_media
                    "cast(? as json)," + // post_itunes
                    "?," + // post_url
                    "cast(? as json)," + // post_urls
                    "?," + // post_img_url
                    "?," + // post_img_transport_ident
                    "?," + // importer_id
                    "?," + // importer_desc
                    "?," + // subscription_id
                    "?," + // queue_id
                    "?," + // import_timestamp
                    "?," + // post_read_status
                    "?," + // post_pub_status
                    "?," + // username
                    "?," + // post_comment
                    "?," + // post_rights
                    "cast(? as json)," + // contributors
                    "cast(? as json)," + // authors
                    "cast(? as json)," + // post_categories
                    "?," + // publish_timestamp
                    "?," + // expiration_timestamp
                    "cast(? as json)," + // enclosures
                    "?," + // last_updated_timestamp
                    "current_timestamp" + // created
                ")";

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date d) {
        Instant i = d != null ? OffsetDateTime.from(d.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return i != null ? Timestamp.from(i) : null;
    }

    /**
     * Adds a new staging post to the database.
     *
     * @param stagingPost The staging post to be added.
     * @return The ID of the newly added staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a data conflict or duplication is encountered during the addition.
     */
    @SuppressWarnings("unused")
    public Long add(StagingPost stagingPost) throws DataAccessException, DataUpdateException, DataConflictException {
        int rowsUpdated;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_STAGING_POST_SQL, new String[]{"id"});
                        ps.setString(1, stagingPost.getPostHash());
                        ps.setString(2, GSON.toJson(stagingPost.getPostTitle()));
                        ps.setString(3, GSON.toJson(stagingPost.getPostDesc()));
                        ps.setString(4, ofNullable(stagingPost.getPostContents()).map(GSON::toJson).orElse(null));
                        ps.setString(5, ofNullable(stagingPost.getPostMedia()).map(GSON::toJson).orElse(null));
                        ps.setString(6, ofNullable(stagingPost.getPostITunes()).map(GSON::toJson).orElse(null));
                        ps.setString(7, stagingPost.getPostUrl());
                        ps.setString(8, ofNullable(stagingPost.getPostUrls()).map(GSON::toJson).orElse(null));
                        ps.setString(9, stagingPost.getPostImgUrl());
                        ps.setString(10, stagingPost.getPostImgTransportIdent());
                        ps.setString(11, stagingPost.getImporterId()); // nn
                        ps.setString(12, stagingPost.getImporterDesc());
                        Long subscriptionId = stagingPost.getSubscriptionId();
                        if (subscriptionId != null) {
                            ps.setLong(13, stagingPost.getSubscriptionId());
                        } else {
                            ps.setNull(13, INTEGER);
                        }
                        ps.setLong(14, stagingPost.getQueueId()); // nn
                        ps.setTimestamp(15, toTimestamp(stagingPost.getImportTimestamp()));
                        ps.setString(16, ofNullable(stagingPost.getPostReadStatus()).map(Enum::name).orElse(null));
                        ps.setString(17, ofNullable(stagingPost.getPostPubStatus()).map(Enum::name).orElse(null));
                        ps.setString(18, stagingPost.getUsername());
                        ps.setString(19, stagingPost.getPostComment());
                        ps.setString(20, stagingPost.getPostRights());
                        ps.setString(21, ofNullable(stagingPost.getContributors()).map(GSON::toJson).orElse(null));
                        ps.setString(22, ofNullable(stagingPost.getAuthors()).map(GSON::toJson).orElse(null));
                        ps.setString(23, ofNullable(stagingPost.getPostCategories()).map(GSON::toJson).orElse(null));
                        ps.setTimestamp(24, toTimestamp(stagingPost.getPublishTimestamp()));
                        ps.setTimestamp(25, toTimestamp(stagingPost.getExpirationTimestamp()));
                        ps.setString(26, ofNullable(stagingPost.getEnclosures()).map(GSON::toJson).orElse(null));
                        ps.setTimestamp(27, toTimestamp(stagingPost.getLastUpdatedTimestamp()));

                        return ps;
                    }, keyHolder);
        } catch (DuplicateKeyException e) {
            throw new DataConflictException(getClass().getSimpleName(), "add", e.getMessage(), stagingPost);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), stagingPost);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", stagingPost);
        }
        return keyHolder.getKeyAs(Long.class);
    }

    private final RowMapper<StagingPost> STAGING_POST_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        // post_title
        ContentObject postTitle = null;
        PGobject postTitleObj = ((PGobject) rs.getObject("post_title"));
        if (postTitleObj != null) {
            postTitle = GSON.fromJson(postTitleObj.getValue(), ContentObject.class);
        }
        // post_description
        ContentObject postDesc = null;
        PGobject postDescObj = ((PGobject) rs.getObject("post_desc"));
        if (postDescObj != null) {
            postDesc = GSON.fromJson(postDescObj.getValue(), ContentObject.class);
        }
        // post_contents
        List<ContentObject> postContents = null;
        PGobject postContentsObj = ((PGobject) rs.getObject("post_contents"));
        if (postContentsObj != null) {
            postContents = GSON.fromJson(postContentsObj.getValue(), LIST_CONTENT_OBJECT_TYPE);
        }
        // post_media
        PostMedia postMedia = null;
        PGobject postMediaObj = ((PGobject) rs.getObject("post_media"));
        if (postMediaObj != null) {
            postMedia = GSON.fromJson(postMediaObj.getValue(), PostMedia.class);
        }
        // post iTunes
        PostITunes postITunes = null;
        PGobject postITunesObj = ((PGobject) rs.getObject("post_itunes"));
        if (postITunesObj != null) {
            postITunes = GSON.fromJson(postITunesObj.getValue(), PostITunes.class);
        }
        // post_url
        String postUrl = rs.getString("post_url");
        // post_urls
        List<PostUrl> postUrls = null;
        PGobject postUrlsObj = ((PGobject) rs.getObject("post_urls"));
        if (postUrlsObj != null) {
            postUrls = GSON.fromJson(postUrlsObj.getValue(), LIST_POST_URL_TYPE);
        }
        String postImgUrl = rs.getString("post_img_url");
        String postImgTransportIdent = rs.getString("post_img_transport_ident");
        String importerId = rs.getString("importer_id");
        Long queueId = rs.getLong("queue_id");
        String importerDesc = rs.getString("importer_desc");
        Long subscriptionId = rs.getLong("subscription_id");
        Timestamp importTimestamp = rs.getTimestamp("import_timestamp");
        String postReadStatus = rs.getString("post_read_status");
        String postPubStatus = rs.getString("post_pub_status");
        String postHash = rs.getString("post_hash");
        String username = rs.getString("username");
        String postComment = rs.getString("post_comment");
        String postRights = rs.getString("post_rights");
        // contributors
        List<PostPerson> contributors = null;
        PGobject contributorsObj = ((PGobject) rs.getObject("contributors"));
        if (contributorsObj != null) {
            contributors = GSON.fromJson(contributorsObj.getValue(), LIST_POST_PERSON_TYPE);
        }
        // authors
        List<PostPerson> authors = null;
        PGobject authorsObj = ((PGobject) rs.getObject("authors"));
        if (authorsObj != null) {
            authors = GSON.fromJson(authorsObj.getValue(), LIST_POST_PERSON_TYPE);
        }
        // post_categories
        List<String> postCategories = null;
        PGobject postCategoriesObj = ((PGobject) rs.getObject("post_categories"));
        if (postCategoriesObj != null) {
            postCategories = GSON.fromJson(postCategoriesObj.getValue(), LIST_STRING_TYPE);
        }
        Timestamp publishTimestamp = rs.getTimestamp("publish_timestamp");
        Timestamp expirationTimestamp = rs.getTimestamp("expiration_timestamp");
        // enclosures
        List<PostEnclosure> enclosures = null;
        PGobject enclosuresObj = ((PGobject) rs.getObject("enclosures"));
        if (enclosuresObj != null) {
            enclosures = GSON.fromJson(enclosuresObj.getValue(), LIST_POST_ENCLOSURE_TYPE);
        }
        Timestamp lastUpdatedTimestamp = rs.getTimestamp("last_updated_timestamp");
        boolean isPublished = rs.getBoolean("is_published");
        Timestamp created = rs.getTimestamp("created");
        Timestamp lastModified = rs.getTimestamp("last_modified");

        StagingPost p = StagingPost.from(
                importerId,
                queueId,
                importerDesc,
                subscriptionId,
                postTitle,
                postDesc,
                postContents,
                postMedia,
                postITunes,
                postUrl,
                postUrls,
                postImgUrl,
                postImgTransportIdent,
                importTimestamp,
                postHash,
                username,
                postComment,
                postRights,
                contributors,
                authors,
                postCategories,
                publishTimestamp,
                expirationTimestamp,
                enclosures,
                lastUpdatedTimestamp,
                created,
                lastModified
        );
        p.setId(id);
        if (postReadStatus != null) {
            try {
                p.setPostReadStatus(PostReadStatus.valueOf(postReadStatus));
            } catch (Exception e) {
                log.error("Unknown post-read status for postId={}, status={}", p.getId(), postReadStatus);
            }
        }
        if (postPubStatus != null) {
            try {
                p.setPostPubStatus(PostPubStatus.valueOf(postPubStatus));
            } catch (Exception e) {
                log.error("Unknown post-pub status for postId={}, status={}", p.getId(), postPubStatus);
            }
        }
        p.setPublished(isPublished);

        return p;
    };

//    private static final String FIND_PUB_PENDING_SQL = "select * from staging_posts where post_pub_status = 'PUB_PENDING'";

    // Note: this query excludes queues marked for deletion
    private static final String FIND_PUB_PENDING_BY_QUEUE_ID_SQL =
            "select * from staging_posts s " +
            "join queue_definitions f on f.id = s.queue_id " +
            "where f.queue_status = 'ENABLED' and f.is_deleted is false and f.username = ? " +
            "and s.post_pub_status = 'PUB_PENDING' " +
            "and s.queue_id = ?";

    /**
     * Retrieves a list of staging posts that are pending publication for a specific user and queue.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue.
     * @return A list of staging posts pending publication.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> getPubPending(String username, Long queueId) throws DataAccessException {
        if (isNotBlank(username) && queueId != null) {
            try {
                return jdbcTemplate.query(FIND_PUB_PENDING_BY_QUEUE_ID_SQL, new Object[]{username, queueId}, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "getPubPending", e.getMessage(), username, queueId);
            }
        }

        return emptyList(); // jdbcTemplate.query(FIND_PUB_PENDING_SQL, STAGING_POST_ROW_MAPPER);
    }

    // Note: this query does not exclude queues marked for deletion
    private static final String FIND_DEPUB_PENDING_BY_QUEUE_SQL =
            "select * from staging_posts s " +
                    "join queue_definitions f on f.id = s.queue_id " +
                    "where f.queue_status = 'ENABLED' and f.username = ? " +
                    "and s.post_pub_status = 'DEPUB_PENDING' " +
                    "and s.queue_id = ?";

    /**
     * Retrieves a list of staging posts that are pending depublishment for a specific user and queue.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue.
     * @return A list of staging posts pending depublishment.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> getDepubPending(String username, Long queueId) throws DataAccessException {
        if (isNotBlank(username) && queueId != null) {
            try {
                return jdbcTemplate.query(FIND_DEPUB_PENDING_BY_QUEUE_SQL, new Object[] { username, queueId }, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "getDepubPending", e.getMessage(), username, queueId);
            }
        }

        return null;
    }

    private static final String DELETE_BY_QUEUE_ID_SQL = "delete from staging_posts where queue_id = ? and username = ?";

    /**
     * Deletes all staging posts associated with a specific user and queue.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void deleteByQueueId(String username, long queueId) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_QUEUE_ID_SQL, queueId, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteByQueueId", e.getMessage(), username, queueId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByQueueId", username, queueId);
        }
    }

    private static final String DELETE_BY_ID_SQL = "delete from staging_posts where id = ? and username = ?";

    /**
     * Deletes a staging post by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to delete.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void deleteById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", username, id);
        }
    }

    /**
     * Deletes multiple staging posts by their IDs for a specific user.
     *
     * @param username The username of the user.
     * @param ids      A list of staging post IDs to delete.
     * @return The number of staging posts deleted.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public int deleteByIds(String username, List<Long> ids) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> params = ids.stream().map(i -> new Object[]{i, username}).collect(toList());
            rowsUpdated = stream(jdbcTemplate.batchUpdate(DELETE_BY_ID_SQL, params)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteByIds", e.getMessage(), username, ids);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByIds", username, ids);
        }

        return rowsUpdated;
    }

    private static final String FIND_QUEUE_ID_BY_STAGING_POST_ID =
            "select queue_id from staging_posts where id = ? and username = ?";

    /**
     * Finds the queue ID associated with a staging post by its ID and username.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post.
     * @return The queue ID of the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public Long findQueueIdByStagingPostId(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_QUEUE_ID_BY_STAGING_POST_ID, new Object[] { id, username }, Long.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findQueueIdentByStagingPostId", e.getMessage(), username, id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from staging_posts";

    /**
     * Retrieves a list of all staging posts.
     *
     * @return A list of all staging posts.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER_SQL = "select s.* from staging_posts s " +
            "join queue_definitions f on f.id = s.queue_id " +
            "where (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED') " +
            "and f.username = ? " +
            "and f.is_deleted is false";

    /**
     * Retrieves a list of staging posts for a specific user.
     *
     * @param username The username of the user.
     * @return A list of staging posts for the user.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    // Note: this query exclude queues marked for deletion
    private static final String FIND_BY_USER_AND_QUEUE_ID_SQL_TEMPLATE =
            "select s.* from staging_posts s " +
                "join queue_definitions f on f.id = s.queue_id " +
                "where f.username = ? " +
                "and f.is_deleted is false " +
                "and f.id in (%s) " +
                "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    /**
     * Retrieves a list of staging posts for a specific user and a list of queue IDs.
     *
     * @param username The username of the user.
     * @param queueIds A list of queue IDs.
     * @return A list of staging posts for the user and specified queue IDs.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findByUserAndQueueIds(String username, List<Long> queueIds) throws DataAccessException {
        try {
            // TODO: this should be a general purpose utility method, and should support groups on 'in' params > 1000
            String inSql = queueIds.stream()
                .map(Object::toString)
                .map(s -> s.replaceAll("[^\\d-]", EMPTY))
                .collect(joining(","));
            return jdbcTemplate.query(
                    String.format(FIND_BY_USER_AND_QUEUE_ID_SQL_TEMPLATE, inSql),
                    new Object[]{username},
                    STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUserAndQueueIds", e.getMessage(), username, queueIds);
        }
    }

    private static final String FIND_BY_USER_AND_SUBSCRIPTION_ID_SQL = "select s.* from staging_posts s " +
            "join queue_definitions f on f.id = s.queue_id " +
            "where (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED') " +
            "and f.username = ? " +
            "and f.is_deleted is false " +
            "and s.subscription_id = ?";

    /**
     * Retrieves a list of staging posts for a specific user and subscription ID.
     *
     * @param username       The username of the user.
     * @param subscriptionId The subscription ID.
     * @return A list of staging posts for the user and specified subscription ID.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findByUserAndSubscriptionId(String username, Long subscriptionId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USER_AND_SUBSCRIPTION_ID_SQL, new Object[]{ username, subscriptionId }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUserAndSubscriptionId", e.getMessage(), username, subscriptionId);
        }
    }

    private static final String FIND_ALL_UNPUBLISHED_SQL = "select s.* from staging_posts s " +
            "join queue_definitions f on f.id = s.queue_id " +
            "where s.is_published = false " +
            "and f.is_deleted is false " +
            "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    /**
     * Retrieves a list of all unpublished staging posts.
     *
     * @return A list of all unpublished staging posts.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findAllUnpublished() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_UNPUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAllUnpublished", e.getMessage());
        }
    }

    private static final String FIND_UNPUBLISHED_BY_USER_SQL = "select s.* from staging_posts s " +
            "join queue_definitions f on f.id = s.queue_id " +
            "where f.username = ? " +
            "and f.is_deleted is false" +
            "and s.is_published is false " +
            "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    /**
     * Retrieves a list of unpublished staging posts for a specific user.
     *
     * @param username The username of the user.
     * @return A list of unpublished staging posts for the user.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findUnpublishedByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_UNPUBLISHED_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findUnpublishedByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_ALL_PUBLISHED_SQL = "select * from staging_posts where is_published = true";

    /**
     * Retrieves a list of all published staging posts.
     *
     * @return A list of all published staging posts.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findAllPublished() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_PUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAllPublished", e.getMessage());
        }
    }

    private static final String FIND_ALL_IDLE_SQL_TEMPLATE =
            "select username,id from staging_posts where " +
                    "( (post_read_status is null and import_timestamp < current_timestamp - INTERVAL '%s DAYS') or " + // post age check for unread
                    "  (post_read_status = 'READ' and import_timestamp < current_timestamp - INTERVAL '%s DAYS')" + // post age check for read
                    ") and post_pub_status != 'PUBLISHED'"; // post pub status check

    /**
     * Retrieves a map of usernames to lists of idle staging post IDs based on specified maximum unread and read ages.
     *
     * @param maxUnreadAge The maximum age (in days) for unread staging posts.
     * @param maxReadAge   The maximum age (in days) for read staging posts.
     * @return A map of usernames to lists of idle staging post IDs.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public Map<String, List<Long>> findAllIdle(int maxUnreadAge, int maxReadAge) throws DataAccessException {
        try {
            // TODO: extract a utility method here
            String maxUnreadAgeStr = Integer.toString(maxUnreadAge).replaceAll("[^\\d-]", EMPTY);
            String maxAgeStr = Integer.toString(maxReadAge).replaceAll("[^\\d-]", EMPTY);
            String sql = String.format(FIND_ALL_IDLE_SQL_TEMPLATE, maxUnreadAgeStr, maxAgeStr);
            return jdbcTemplate.query(sql, rs -> {
                Map<String, List<Long>> m = new HashMap<>();
                while (rs.next()) {
                    String username = rs.getString("username");
                    Long postId = rs.getLong("id");
                    m.computeIfAbsent(username, id -> new ArrayList<>()).add(postId);
                }

                return m;
            });
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAllIdle", e.getMessage(), maxUnreadAge, maxReadAge);
        }
    }

    private static final String FIND_PUBLISHED_BY_USER_SQL = "select * from staging_posts where is_published = true and username = ?";

    /**
     * Retrieves a list of published staging posts for a specific user.
     *
     * @param username The username of the user.
     * @return A list of published staging posts for the user.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findPublishedByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_PUBLISHED_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findPublishedByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_PUBLISHED_BY_QUEUE_SQL =
            "select * from staging_posts " +
                    "where is_published = true " +
                    "and (post_pub_status is null or post_pub_status != 'DEPUB_PENDING') " +
                    "and queue_id = ? and username = ?";

    /**
     * Retrieves a list of published staging posts for a specific user and queue.
     *
     * @param username The username of the user.
     * @param queueId  The ID of the queue.
     * @return A list of published staging posts for the user and specified queue.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<StagingPost> findPublishedByQueue(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_PUBLISHED_BY_QUEUE_SQL, new Object[] { queueId, username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findPublishedByQueue", e.getMessage(), username, queueId);
        }
    }

    private static final String FIND_BY_ID_SQL = "select * from staging_posts where id = ? and username = ?";

    /**
     * Retrieves a staging post by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to retrieve.
     * @return The staging post with the specified ID.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public StagingPost findById(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_ID_SQL, new Object[] { id, username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), username, id);
        }
    }

    private static final String CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE = "select is_published from staging_posts where id = %s and username = ?";

    /**
     * Checks if a staging post with the specified ID is published for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to check.
     * @return `true` if the staging post is published, `false` otherwise.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public Boolean checkPublished(String username, long id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE, String.valueOf(id).replaceAll("\\D", ""));
            return jdbcTemplate.queryForObject(sql, new Object[]{username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "checkPublished", e.getMessage(), username, id);
        }
    }

    private static final String MARK_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_pub_status = null, is_published = true, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Marks a staging post as published and complete for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to mark as complete.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void markPubComplete(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_PUB_COMPLETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "markPubComplete", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "markPubComplete", username, id);
        }
    }

    private static final String CLEAR_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_pub_status = null, is_published = false, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the published and complete status of a staging post for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to clear the status.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPubComplete(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_PUB_COMPLETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPubComplete", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPubComplete", username, id);
        }
    }

    private static final String UPDATE_POST_READ_STATUS_BY_ID = "update staging_posts set post_read_status = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the read status of a staging post for a specific user.
     *
     * @param username   The username of the user.
     * @param id         The ID of the staging post to update.
     * @param postStatus The new read status of the staging post.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostReadStatus(String username, long id, PostReadStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_READ_STATUS_BY_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostReadStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostReadStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_READ_STATUS_BY_QUEUE_ID = "update staging_posts set post_read_status = ?, last_modified = current_timestamp where queue_id = ? and username = ?";

    /**
     * Updates the read status of all staging posts in a queue for a specific user.
     *
     * @param username   The username of the user.
     * @param id         The ID of the queue to update staging posts in.
     * @param postStatus The new read status for the staging posts in the queue.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updateQueueReadStatus(String username, long id, PostReadStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_READ_STATUS_BY_QUEUE_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueueReadStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateQueueReadStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_PUB_STATUS_BY_ID = "update staging_posts set post_pub_status = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the publication status of a staging post for a specific user.
     *
     * @param username   The username of the user.
     * @param id         The ID of the staging post to update.
     * @param postStatus The new publication status of the staging post.
     * @throws DataAccessException    If an error occurs while accessing the data.
     * @throws DataUpdateException   If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostPubStatus(String username, long id, PostPubStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_PUB_STATUS_BY_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostPubStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostPubStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_PUB_STATUS_BY_QUEUE_ID = "update staging_posts set post_pub_status = ?, last_modified = current_timestamp where queue_id = ? and username = ? and post_pub_status is not null";

    /**
     * Updates the publication status of all staging posts in a queue for a specific user.
     *
     * @param username   The username of the user.
     * @param id         The ID of the queue to update staging posts in.
     * @param postStatus The new publication status for the staging posts in the queue.
     * @throws DataAccessException    If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public void updateQueuePubStatus(String username, long id, PostPubStatus postStatus) throws DataAccessException {
        try {
            jdbcTemplate.update(UPDATE_POST_PUB_STATUS_BY_QUEUE_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateQueuePubStatus", e.getMessage(), username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_BY_ID_TEMPLATE = "update staging_posts set %s where id = ? and username = ?";

    /**
     * Updates a staging post with various attributes.
     *
     * @param mergeUpdate        Flag indicating whether to merge updates with existing attributes.
     * @param username           The username of the user.
     * @param id                 The ID of the staging post to update.
     * @param postTitle          The title of the staging post.
     * @param postDesc           The description of the staging post.
     * @param postContents       The contents of the staging post.
     * @param postMedia          The media associated with the staging post.
     * @param postITunes         The iTunes-specific information for the staging post.
     * @param postUrl            The URL of the staging post.
     * @param postUrls           The URLs associated with the staging post.
     * @param postImgUrl         The image URL of the staging post.
     * @param postComment        The comment for the staging post.
     * @param postRights         The rights information for the staging post.
     * @param contributors       The contributors to the staging post.
     * @param authors            The authors of the staging post.
     * @param postCategories     The categories associated with the staging post.
     * @param expirationTimestamp The expiration timestamp of the staging post.
     * @param enclosures         The enclosures associated with the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePost(boolean mergeUpdate, String username, long id, ContentObject postTitle, ContentObject postDesc,
                           List<ContentObject> postContents, PostMedia postMedia, PostITunes postITunes, String postUrl,
                           List<PostUrl> postUrls, String postImgUrl, String postComment, String postRights,
                           List<PostPerson> contributors, List<PostPerson> authors, List<String> postCategories,
                           Date expirationTimestamp, List<PostEnclosure> enclosures)
            throws DataAccessException, DataUpdateException
    {
        // assemble update statement arguments
        List<Object> updateArgs = new ArrayList<>();
        //
        // start with simple attributes
        //
        List<String> simpleUpdateAttrs = new ArrayList<>();
        if (postUrl != null) {
            updateArgs.add(postUrl);
            simpleUpdateAttrs.add("post_url");
        }
        if (postImgUrl != null) {
            //
            updateArgs.add(postImgUrl);
            simpleUpdateAttrs.add("post_img_url");
            //
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                updateArgs.add(computeThumbnailHash(md, postImgUrl));
                simpleUpdateAttrs.add("post_img_transport_ident");
            } catch (NoSuchAlgorithmException ignored) {
                // ignored
            }
        }
        if (postComment != null) {
            updateArgs.add(postComment);
            simpleUpdateAttrs.add("post_comment");
        }
        if (postRights != null) {
            updateArgs.add(postRights);
            simpleUpdateAttrs.add("post_rights");
        }
        // expiration timestamp
        if (expirationTimestamp != null) {
            updateArgs.add(expirationTimestamp);
            simpleUpdateAttrs.add("expiration_timestamp");
        }
        //
        // now JSON attributes
        //
        List<String> jsonUpdateAttrs = new ArrayList<>();
        // post_title
        if (postTitle != null) {
            updateArgs.add(GSON.toJson(postTitle));
            jsonUpdateAttrs.add("post_title");
        }
        // post_desc
        if (postDesc != null) {
            updateArgs.add(GSON.toJson(postDesc));
            jsonUpdateAttrs.add("post_desc");
        }
        // post_contents
        if (postContents != null) {
            updateArgs.add(GSON.toJson(postContents));
            jsonUpdateAttrs.add("post_contents");
        }
        // post_media
        if (postMedia != null) {
            updateArgs.add(GSON.toJson(postMedia));
            jsonUpdateAttrs.add("post_media");
        }
        // post_itunes
        if (postITunes != null) {
            updateArgs.add(GSON.toJson(postITunes));
            jsonUpdateAttrs.add("post_itunes");
        }
        // post_urls
        if (postUrls != null) {
            updateArgs.add(GSON.toJson(postUrls));
            jsonUpdateAttrs.add("post_urls");
        }
        // post_categories
        if (postCategories != null) {
            updateArgs.add(GSON.toJson(postCategories));
            jsonUpdateAttrs.add("post_categories");
        }
        // contributors
        if (contributors != null) {
            updateArgs.add(GSON.toJson(contributors));
            jsonUpdateAttrs.add("contributors");
        }
        // authors
        if (authors != null) {
            updateArgs.add(GSON.toJson(authors));
            jsonUpdateAttrs.add("authors");
        }
        // enclosures
        if (enclosures != null) {
            updateArgs.add(GSON.toJson(enclosures));
            jsonUpdateAttrs.add("enclosures");
        }
        updateArgs.add(id);
        updateArgs.add(username);
        // assemble the final update statement
        List<String> allUpdateAttrs = new ArrayList<>();
        allUpdateAttrs.addAll(simpleUpdateAttrs.stream().map(a -> a + "=?").toList());
        allUpdateAttrs.addAll(jsonUpdateAttrs.stream().map(j -> j + "=?::json").toList());
        if (isNotEmpty(allUpdateAttrs)) {
            String updateClause = String.join(",", allUpdateAttrs);
            updateClause = updateClause + ", last_modified = current_timestamp ";
            String updateSql = String.format(UPDATE_POST_BY_ID_TEMPLATE, updateClause);
            // perform the update
            int rowsUpdated;
            try {
                rowsUpdated = jdbcTemplate.update(updateSql, updateArgs.toArray());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updatePost attrs=" + simpleUpdateAttrs, e.getMessage(), updateArgs.toArray());
            }
            // check the result
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePost attrs=" + simpleUpdateAttrs, updateArgs.toArray());
            }
        }
    }

    private static final String UPDATE_POST_TITLE_BY_ID = "update staging_posts set post_title = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the title of a staging post.
     *
     * @param mergeUpdate Flag indicating whether to merge updates with the existing title.
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postTitle   The new title for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostTitle(boolean mergeUpdate, String username, long id, ContentObject postTitle) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_TITLE_BY_ID, postTitle == null ? null : GSON.toJson(postTitle), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostTitle", e.getMessage(), username, id, postTitle);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostTitle", username, id, postTitle);
        }
    }

    private static final String UPDATE_POST_DESC_BY_ID = "update staging_posts set post_desc = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the description of a staging post.
     *
     * @param mergeUpdate Flag indicating whether to merge updates with the existing description.
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postDesc    The new description for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostDesc(boolean mergeUpdate, String username, long id, ContentObject postDesc) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_DESC_BY_ID, postDesc == null ? null : GSON.toJson(postDesc), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostDesc", e.getMessage(), username, id, postDesc);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostDesc", username, id, postDesc);
        }
    }

    private static final String UPDATE_POST_ITUNES_BY_ID = "update staging_posts set post_itunes = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the iTunes descriptor of a staging post.
     *
     * @param mergeUpdate Flag indicating whether to merge updates with the existing iTunes descriptor.
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postITunes  The new iTunes descriptor for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostITunes(boolean mergeUpdate, String username, long id, PostITunes postITunes) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_ITUNES_BY_ID, postITunes == null ? null : GSON.toJson(postITunes), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostITunes", e.getMessage(), username, id, postITunes);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostITunes", username, id, postITunes);
        }
    }

    private static final String UPDATE_POST_COMMNENT_BY_ID = "update staging_posts set post_comment = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the comment URL of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postComment The new comment URL for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostComment(String username, long id, String postComment) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_COMMNENT_BY_ID, postComment, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostComment", e.getMessage(), username, id, postComment);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostComment", username, id, postComment);
        }
    }

    private static final String UPDATE_POST_RIGHTS_BY_ID = "update staging_posts set post_rights = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the rights string of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postRights  The new rights string for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostRights(String username, long id, String postRights) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_RIGHTS_BY_ID, postRights, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostRights", e.getMessage(), username, id, postRights);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostRights", username, id, postRights);
        }
    }

    private static final String UPDATE_EXPIRATION_TIMESTAMP_BY_ID = "update staging_posts set expiration_timestamp = ?, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the expiration timestamp of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param expirationTimestamp The new expiration timestamp for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updateExpirationTimestamp(String username, long id, Date expirationTimestamp) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_EXPIRATION_TIMESTAMP_BY_ID, expirationTimestamp, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateExpirationTimestamp", e.getMessage(), username, id, expirationTimestamp);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateExpirationTimestamp", username, id, expirationTimestamp);
        }
    }

    private static final String UPDATE_POST_CONTENTS_BY_ID = "update staging_posts set post_contents = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the contents of a staging post.
     *
     * @param username     The username of the user.
     * @param id           The ID of the staging post to update.
     * @param postContents The new contents for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostContents(String username, long id, List<ContentObject> postContents) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_CONTENTS_BY_ID, postContents == null ? null : GSON.toJson(postContents), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostContents", e.getMessage(), username, id, postContents);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostContents", username, id, postContents);
        }
    }

    private static final String UPDATE_POST_URLS_BY_ID = "update staging_posts set post_urls = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the URLs of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postUrls    The new URLs for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostUrls(String username, long id, List<PostUrl> postUrls) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_URLS_BY_ID, postUrls == null ? null : GSON.toJson(postUrls), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostUrls", e.getMessage(), username, id, postUrls);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostUrls", username, id, postUrls);
        }
    }

    private static final String UPDATE_CONTRIBUTORS_BY_ID = "update staging_posts set contributors = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the contributors of a staging post.
     *
     * @param username     The username of the user.
     * @param id           The ID of the staging post to update.
     * @param contributors The new contributors for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updateContributors(String username, long id, List<PostPerson> contributors) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_CONTRIBUTORS_BY_ID, contributors == null ? null : GSON.toJson(contributors), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateContributors", e.getMessage(), username, id, contributors);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateContributors", username, id, contributors);
        }
    }

    private static final String UPDATE_AUTHORS_BY_ID = "update staging_posts set authors = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the authors of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param authors     The new authors for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updateAuthors(String username, long id, List<PostPerson> authors) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_AUTHORS_BY_ID, authors == null ? null : GSON.toJson(authors), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateAuthors", e.getMessage(), username, id, authors);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateAuthors", username, id, authors);
        }
    }

    private static final String UPDATE_POST_ENCLOSURES_BY_ID = "update staging_posts set enclosures = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the enclosures of a staging post.
     *
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param enclosures  The new enclosures for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostEnclosures(String username, long id, List<PostEnclosure> enclosures) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_ENCLOSURES_BY_ID, enclosures == null ? null : GSON.toJson(enclosures), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostEnclosures", e.getMessage(), username, id, enclosures);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostEnclosures", username, id, enclosures);
        }
    }

    private static final String UPDATE_POST_MEDIA_BY_ID = "update staging_posts set post_media = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the media descriptor of a staging post.
     *
     * @param mergeUpdate Flag indicating whether to merge updates with the existing media module descriptor.
     * @param username    The username of the user.
     * @param id          The ID of the staging post to update.
     * @param postMedia   The new media descriptor for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostMedia(boolean mergeUpdate, String username, long id, PostMedia postMedia) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_MEDIA_BY_ID, postMedia == null ? null : GSON.toJson(postMedia), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostMedia", e.getMessage(), username, id, postMedia);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostMedia", username, id, postMedia);
        }
    }

    private static final String UPDATE_POST_CATEGORIES_BY_ID = "update staging_posts set post_categories = ?::json, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Updates the categories of a staging post.
     *
     * @param username       The username of the user.
     * @param id             The ID of the staging post to update.
     * @param postCategories The new categories for the staging post.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void updatePostCategories(String username, long id, List<String> postCategories) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_CATEGORIES_BY_ID, postCategories == null ? null : GSON.toJson(postCategories), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePostCategories", e.getMessage(), username, id, postCategories);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostCategories", username, id, postCategories);
        }
    }

    private static final String CLEAR_POST_ITUNES_BY_ID_SQL = "update staging_posts set post_itunes = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the iTunes information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostITunes(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_ITUNES_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostITunes", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostITunes", username, id);
        }
    }

    private static final String CLEAR_POST_COMMENT_BY_ID_SQL = "update staging_posts set post_itunes = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the comment of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostComment(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_COMMENT_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostITunes", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostITunes", username, id);
        }
    }

    private static final String CLEAR_POST_RIGHTS_BY_ID_SQL = "update staging_posts set post_itunes = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the rights information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostRights(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_RIGHTS_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostITunes", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostITunes", username, id);
        }
    }

    private static final String CLEAR_EXPIRATION_TIMESTAMP_BY_ID_SQL = "update staging_posts set post_itunes = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the expiration timestamp of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearExpirationTimestamp(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_EXPIRATION_TIMESTAMP_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostITunes", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostITunes", username, id);
        }
    }

    private static final String CLEAR_POST_MEDIA_BY_ID_SQL = "update staging_posts set post_media = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the media information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostMedia(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_MEDIA_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostMedia", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostMedia", username, id);
        }
    }

    private static final String CLEAR_POST_CONTENTS_BY_ID_SQL = "update staging_posts set post_contentsw = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the contents of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostContents(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_CONTENTS_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostContents", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostContents", username, id);
        }
    }

    private static final String CLEAR_POST_URLS_BY_ID_SQL = "update staging_posts set post_urls = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the URLs information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostUrls(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_URLS_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostUrls", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostUrls", username, id);
        }
    }

    private static final String CLEAR_POST_AUTHORS_BY_ID_SQL = "update staging_posts set authors = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the authors information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostAuthors(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_AUTHORS_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostAuthors", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostAuthors", username, id);
        }
    }

    private static final String CLEAR_POST_CONTRIBUTORS_BY_ID_SQL = "update staging_posts set contributors = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the contributors information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostContributors(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_CONTRIBUTORS_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostContributors", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostContributors", username, id);
        }
    }

    private static final String CLEAR_POST_CATEGORIES_BY_ID_SQL = "update staging_posts set post_categories = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the categories information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostCategories(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_CATEGORIES_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostCategories", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostCategories", username, id);
        }
    }

    private static final String CLEAR_POST_ENCLOSURES_BY_ID_SQL = "update staging_posts set enclosures = null, last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Clears the enclosures information of a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void clearPostEnclosures(String username, Long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_POST_ENCLOSURES_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "clearPostEnclosures", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPostEnclosures", username, id);
        }
    }

    private static final String ARCHIVE_BY_ID_SQL = "update staging_posts set post_pub_status = 'ARCHIVE', last_modified = current_timestamp where id = ? and username = ?";

    /**
     * Archives a staging post identified by its ID for a specific user.
     *
     * @param username The username of the user.
     * @param id       The ID of the staging post to update.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void archiveById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(ARCHIVE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "archiveById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "archiveById", username, id);
        }
    }

    /**
     * Archives staging posts by their IDs for a specific user.
     *
     * @param username The username of the user.
     * @param ids      The list of IDs of staging posts to be archived.
     * @return The number of staging posts successfully archived.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public int archiveByIds(String username, List<Long> ids) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> params = ids.stream().map(i -> new Object[]{i, username}).collect(toList());
            rowsUpdated = stream(jdbcTemplate.batchUpdate(ARCHIVE_BY_ID_SQL, params)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "archiveByIds", e.getMessage(), username, ids);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "archiveByIds", username, ids);
        }

        return rowsUpdated;
    }

    private static final String PURGE_ARCHIVED_POSTS_SQL_TEMPLATE = "delete from staging_posts where post_pub_status = 'ARCHIVED' and import_timestamp < current_timestamp - INTERVAL '%s DAYS'";

    /**
     * Purges archived staging posts older than a specified maximum age.
     *
     * @param maxAge The maximum age (in days) of archived posts to retain.
     * @return The number of archived staging posts successfully purged.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public int purgeArchivedPosts(int maxAge) throws DataAccessException {
        int rowsUpdated;
        try {
            String sql = String.format(PURGE_ARCHIVED_POSTS_SQL_TEMPLATE, maxAge);
            rowsUpdated = jdbcTemplate.update(sql);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "purgeArchivedPosts", e.getMessage());
        }

        return rowsUpdated;
    }
}
