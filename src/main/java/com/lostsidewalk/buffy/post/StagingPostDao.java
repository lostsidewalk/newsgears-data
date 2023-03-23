package com.lostsidewalk.buffy.post;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.post.StagingPost.PostPubStatus;
import com.lostsidewalk.buffy.post.StagingPost.PostReadStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
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
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

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

    @SuppressWarnings("unused")
    Boolean checkExists(String stagingPostHash) throws DataAccessException {
        try {
            String sql = String.format(CHECK_EXISTS_BY_HASH_SQL_TEMPLATE, stagingPostHash);
            return jdbcTemplate.queryForObject(sql, null, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
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
                    "query_id," +
                    "feed_id," +
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
                    "last_updated_timestamp" +
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
                    "?," + // query_id
                    "?," + // feed_id
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
                    "?" + // last_updated_timestamp
                ")";

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date d) {
        Instant i = d != null ? OffsetDateTime.from(d.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return i != null ? Timestamp.from(i) : null;
    }

    @SuppressWarnings("unused")
    public Long add(StagingPost stagingPost) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_STAGING_POST_SQL, new String[] { "id" });
                        ps.setString(1, stagingPost.getPostHash());
                        ps.setString(2, GSON.toJson(stagingPost.getPostTitle()));
                        ps.setString(3, GSON.toJson(stagingPost.getPostDesc()));
                        ps.setString(4, ofNullable(stagingPost.getPostContents()).map(GSON::toJson).orElse(null));
                        ps.setString(5, ofNullable(stagingPost.getPostMedia()).map(GSON::toJson).orElse(null));
                        ps.setString(6, ofNullable(stagingPost.getPostITunes()).map(GSON::toJson).orElse(null));
                        ps.setString(7, stagingPost.getPostUrl()); // nn
                        ps.setString(8, ofNullable(stagingPost.getPostUrls()).map(GSON::toJson).orElse(null));
                        ps.setString(9, stagingPost.getPostImgUrl());
                        ps.setString(10, stagingPost.getPostImgTransportIdent());
                        ps.setString(11, stagingPost.getImporterId()); // nn
                        ps.setString(12, stagingPost.getImporterDesc());
                        ps.setLong(13, stagingPost.getQueryId()); // nn
                        ps.setLong(14, stagingPost.getFeedId()); // nn
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
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
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
        Long feedId = rs.getLong("feed_id");
        String importerDesc = rs.getString("importer_desc");
        Long queryId = rs.getLong("query_id");
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

        StagingPost p = StagingPost.from(
                importerId,
                feedId,
                importerDesc,
                queryId,
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
                lastUpdatedTimestamp
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

    // Note: this query excludes feeds marked for deletion
    private static final String FIND_PUB_PENDING_BY_FEED_SQL =
            "select * from staging_posts s " +
            "join feed_definitions f on f.id = s.feed_id " +
            "where f.feed_status = 'ENABLED' and f.is_deleted is false and f.username = ? " +
            "and s.post_pub_status = 'PUB_PENDING' " +
            "and s.feed_id = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> getPubPending(String username, Long feedId) throws DataAccessException {
        if (isNotBlank(username) && feedId != null) {
            try {
                return jdbcTemplate.query(FIND_PUB_PENDING_BY_FEED_SQL, new Object[]{username, feedId}, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "getPubPending", e.getMessage(), username, feedId);
            }
        }

        return emptyList(); // jdbcTemplate.query(FIND_PUB_PENDING_SQL, STAGING_POST_ROW_MAPPER);
    }

    // Note: this query does not exclude feeds marked for deletion
    private static final String FIND_DEPUB_PENDING_BY_FEED_SQL =
            "select * from staging_posts s " +
                    "join feed_definitions f on f.id = s.feed_id " +
                    "where f.feed_status = 'ENABLED' and f.username = ? " +
                    "and s.post_pub_status = 'DEPUB_PENDING' " +
                    "and s.feed_id = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> getDepubPending(String username, Long feedId) throws DataAccessException {
        if (isNotBlank(username) && feedId != null) {
            try {
                return jdbcTemplate.query(FIND_DEPUB_PENDING_BY_FEED_SQL, new Object[] { username, feedId }, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "getDepubPending", e.getMessage(), username, feedId);
            }
        }

        return null;
    }

    private static final String DELETE_BY_ID_SQL = "delete from staging_posts where id = ? and username = ?";

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
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", username, id);
        }
    }

    @SuppressWarnings("unused")
    public int deleteByIds(String username, List<Long> ids) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> params = ids.stream().map(i -> new Object[]{i, username}).collect(toList());
            rowsUpdated = stream(jdbcTemplate.batchUpdate(DELETE_BY_ID_SQL, params)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteByIds", e.getMessage(), username, ids);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByIds", username, ids);
        }

        return rowsUpdated;
    }

    private static final String FIND_FEED_ID_BY_STAGING_POST_ID =
            "select feed_id from staging_posts where id = ? and username = ?";

    @SuppressWarnings("unused")
    public Long findFeedIdByStagingPostId(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_FEED_ID_BY_STAGING_POST_ID, new Object[] { id, username }, Long.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findFeedIdentByStagingPostId", e.getMessage(), username, id);
        }
    }

    private static final String FIND_ALL_SQL = "select * from staging_posts";

    @SuppressWarnings("unused")
    List<StagingPost> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_BY_USER_SQL = "select s.* from staging_posts s " +
            "join feed_definitions f on f.id = s.feed_id " +
            "where (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED') " +
            "and f.username = ? " +
            "and f.is_deleted is false";

    // non-archived only
    @SuppressWarnings("unused")
    public List<StagingPost> findByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    // Note: this query exclude feeds marked for deletion
    private static final String FIND_BY_USER_AND_FEED_ID_SQL_TEMPLATE =
            "select s.* from staging_posts s " +
                "join feed_definitions f on f.id = s.feed_id " +
                "where f.username = ? " +
                "and f.is_deleted is false " +
                "and f.id in (%s) " +
                "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    // non-archived only
    @SuppressWarnings("unused")
    public List<StagingPost> findByUserAndFeedIds(String username, List<Long> feedIds) throws DataAccessException {
        try {
            // TODO: this should be a general purpose utility method, and should support groups on 'in' params > 1000
            String inSql = feedIds.stream()
                .map(Object::toString)
                .map(s -> s.replaceAll("[^\\d-]", EMPTY))
                .collect(joining(","));
            return jdbcTemplate.query(
                    String.format(FIND_BY_USER_AND_FEED_ID_SQL_TEMPLATE, inSql),
                    new Object[]{username},
                    STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUserAndFeedIds", e.getMessage(), username, feedIds);
        }
    }

    private static final String FIND_ALL_UNPUBLISHED_SQL = "select s.* from staging_posts s " +
            "join feed_definitions f on f.id = s.feed_id " +
            "where s.is_published = false " +
            "and f.is_deleted is false " +
            "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    // non-archived only
    @SuppressWarnings("unused")
    List<StagingPost> findAllUnpublished() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_UNPUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllUnpublished", e.getMessage());
        }
    }

    private static final String FIND_UNPUBLISHED_BY_USER_SQL = "select s.* from staging_posts s " +
            "join feed_definitions f on f.id = s.feed_id " +
            "where f.username = ? " +
            "and f.is_deleted is false" +
            "and s.is_published is false " +
            "and (s.post_pub_status is null or s.post_pub_status != 'ARCHIVED')";

    // non-archived only
    @SuppressWarnings("unused")
    List<StagingPost> findUnpublishedByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_UNPUBLISHED_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findUnpublishedByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_ALL_PUBLISHED_SQL = "select * from staging_posts where is_published = true";

    @SuppressWarnings("unused")
    List<StagingPost> findAllPublished() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_PUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllPublished", e.getMessage());
        }
    }

    private static final String FIND_ALL_IDLE_SQL_TEMPLATE =
            "select username,id from staging_posts where " +
                    "( (post_read_status is null and import_timestamp < current_timestamp - INTERVAL '%s DAYS') or " + // post age check for unread
                    "  (post_read_status = 'READ' and import_timestamp < current_timestamp - INTERVAL '%s DAYS')" + // post age check for read
                    ") and post_pub_status != 'PUBLISHED'"; // post pub status check

    @SuppressWarnings("unused")
    Map<String, List<Long>> findAllIdle(int maxUnreadAge, int maxReadAge) throws DataAccessException {
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
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllIdle", e.getMessage(), maxUnreadAge, maxReadAge);
        }
    }

    private static final String FIND_PUBLISHED_BY_USER_SQL = "select * from staging_posts where is_published = true and username = ?";

    @SuppressWarnings("unused")
    List<StagingPost> findPublishedByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_PUBLISHED_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findPublishedByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_PUBLISHED_BY_FEED_SQL =
            "select * from staging_posts " +
                    "where is_published = true " +
                    "and (post_pub_status is null or post_pub_status != 'DEPUB_PENDING') " +
                    "and feed_id = ? and username = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> findPublishedByFeed(String username, Long feedId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_PUBLISHED_BY_FEED_SQL, new Object[] { feedId, username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findPublishedByFeed", e.getMessage(), username, feedId);
        }
    }

    private static final String FIND_BY_ID_SQL = "select * from staging_posts where id = ? and username = ?";

    @SuppressWarnings("unused")
    public StagingPost findById(String username, Long id) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_ID_SQL, new Object[] { id, username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), username, id);
        }
    }

    private static final String CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE = "select is_published from staging_posts where id = %s and username = ?";

    @SuppressWarnings("unused")
    public Boolean checkPublished(String username, long id) throws DataAccessException {
        try {
            String sql = String.format(CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE, String.valueOf(id).replaceAll("\\D", ""));
            return jdbcTemplate.queryForObject(sql, new Object[]{username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkPublished", e.getMessage(), username, id);
        }
    }

    private static final String MARK_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_pub_status = null, is_published = true where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void markPubComplete(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(MARK_PUB_COMPLETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "markPubComplete", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "markPubComplete", username, id);
        }
    }

    private static final String CLEAR_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_pub_status = null, is_published = false where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void clearPubComplete(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(CLEAR_PUB_COMPLETE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "clearPubComplete", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "clearPubComplete", username, id);
        }
    }

    private static final String UPDATE_POST_READ_STATUS_BY_ID = "update staging_posts set post_read_status = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePostReadStatus(String username, long id, PostReadStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_READ_STATUS_BY_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updatePostReadStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostReadStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_READ_STATUS_BY_FEED_ID = "update staging_posts set post_read_status = ? where feed_id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updateFeedReadStatus(String username, long id, PostReadStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_READ_STATUS_BY_FEED_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateFeedReadStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updateFeedReadStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_PUB_STATUS_BY_ID = "update staging_posts set post_pub_status = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePostPubStatus(String username, long id, PostPubStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_PUB_STATUS_BY_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updatePostPubStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostPubStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_PUB_STATUS_BY_FEED_ID = "update staging_posts set post_pub_status = ? where feed_id = ? and username = ? and post_pub_status is not null";

    @SuppressWarnings("unused")
    public void updateFeedPubStatus(String username, long id, PostPubStatus postStatus) throws DataAccessException {
        try {
            jdbcTemplate.update(UPDATE_POST_PUB_STATUS_BY_FEED_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateFeedPubStatus", e.getMessage(), username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_BY_ID_TEMPLATE = "update staging_posts set %s where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePost(String username, long id, ContentObject postTitle, ContentObject postDesc,
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
        // last updated timestamp
        updateArgs.add(toTimestamp(new Date()));
        simpleUpdateAttrs.add("last_updated_timestamp");
        // enclosure URL
        updateArgs.add(id);
        updateArgs.add(username);
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
        // assemble the final update statement
        List<String> allUpdateAttrs = new ArrayList<>();
        allUpdateAttrs.addAll(simpleUpdateAttrs.stream().map(a -> a + "=?").toList());
        allUpdateAttrs.addAll(jsonUpdateAttrs.stream().map(j -> j + "=?::json").toList());
        String updateClause = String.join(",", allUpdateAttrs);
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

    private static final String ARCHIVE_BY_ID_SQL = "update staging_posts set post_pub_status = 'ARCHIVE' where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void archiveById(String username, long id) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(ARCHIVE_BY_ID_SQL, id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "archiveById", e.getMessage(), username, id);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "archiveById", username, id);
        }
    }

    @SuppressWarnings("unused")
    public int archiveByIds(String username, List<Long> ids) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            List<Object[]> params = ids.stream().map(i -> new Object[]{i, username}).collect(toList());
            rowsUpdated = stream(jdbcTemplate.batchUpdate(ARCHIVE_BY_ID_SQL, params)).sum();
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "archiveByIds", e.getMessage(), username, ids);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "archiveByIds", username, ids);
        }

        return rowsUpdated;
    }

    private static final String PURGE_ARCHIVED_POSTS_SQL = "delete from staging_posts where post_pub_status = 'ARCHIVED'";

    @SuppressWarnings("unused")
    public int purgeArchivePosts() throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(PURGE_ARCHIVED_POSTS_SQL);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "purgeArchivedPosts", e.getMessage());
        }

        return rowsUpdated;
    }
}
