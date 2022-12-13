package com.lostsidewalk.buffy.post;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.post.StagingPost.PostStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

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
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.*;

@Slf4j
@Component
public class StagingPostDao {

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
                    "post_url," +
                    "post_img_url," +
                    "post_img_transport_ident," +
                    "importer_id," +
                    "feed_ident," +
                    "object_source," +
                    "import_timestamp," +
                    "post_status," +
                    "username," +
                    "post_comment," +
                    "post_rights," +
                    "xml_base," +
                    "contributor_name," +
                    "contributor_email," +
                    "author_name," +
                    "author_email," +
                    "post_category," +
                    "publish_timestamp," +
                    "expiration_timestamp," +
                    "enclosure_url," +
                    "last_updated_timestamp" +
                    ") values " +
                    "(?,?,?,?,?,?,?,?,cast(? as json),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date d) {
        Instant i = d != null ? OffsetDateTime.from(d.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return i != null ? Timestamp.from(i) : null;
    }

    @SuppressWarnings("unused")
    public Long add(StagingPost stagingPost) throws DataAccessException {
        try {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            int rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_STAGING_POST_SQL, new String[] { "id" });
                        ps.setString(1, stagingPost.getPostHash());
                        ps.setString(2, stagingPost.getPostTitle());
                        ps.setString(3, stagingPost.getPostDesc());
                        ps.setString(4, stagingPost.getPostUrl());
                        ps.setString(5, stagingPost.getPostImgUrl());
                        ps.setString(6, stagingPost.getPostImgTransportIdent());
                        ps.setString(7, stagingPost.getImporterId());
                        ps.setString(8, stagingPost.getFeedIdent());
                        ps.setString(9, stagingPost.getSourceObj().toString());
                        ps.setTimestamp(10, toTimestamp(stagingPost.getImportTimestamp()));
                        ps.setString(11, ofNullable(stagingPost.getPostStatus()).map(Enum::name).orElse(null));
                        ps.setString(12, stagingPost.getUsername());
                        ps.setString(13, stagingPost.getPostComment());
                        ps.setString(14, stagingPost.getPostRights());
                        ps.setString(15, stagingPost.getXmlBase());
                        ps.setString(16, stagingPost.getContributorName());
                        ps.setString(17, stagingPost.getContributorEmail());
                        ps.setString(18, stagingPost.getAuthorName());
                        ps.setString(19, stagingPost.getAuthorEmail());
                        ps.setString(20, stagingPost.getPostCategory());
                        ps.setTimestamp(21, toTimestamp(stagingPost.getPublishTimestamp()));
                        ps.setTimestamp(22, toTimestamp(stagingPost.getExpirationTimestamp()));
                        ps.setString(23, stagingPost.getEnclosureUrl());
                        ps.setTimestamp(24, toTimestamp(stagingPost.getLastUpdatedTimestamp()));

                        return ps;
                    }, keyHolder);

            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "add", stagingPost);
            }
            return keyHolder.getKeyAs(Long.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), stagingPost);
        }
    }

    private final RowMapper<StagingPost> STAGING_POST_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String postTitle = rs.getString("post_title");
        String postDesc = rs.getString("post_desc");
        String postUrl = rs.getString("post_url");
        String postImgUrl = rs.getString("post_img_url");
        String postImgTransportIdent = rs.getString("post_img_transport_ident");
        String importerId = rs.getString("importer_id");
        String feedIdent = rs.getString("feed_ident");
        String sourceObj = ((PGobject) rs.getObject("object_source")).getValue();
        String sourceName = "";
        String sourceUrl = "";
        Timestamp importTimestamp = rs.getTimestamp("import_timestamp");
        String postStatus = rs.getString("post_status");
        String postHash = rs.getString("post_hash");
        String username = rs.getString("username");
        String postComment = rs.getString("post_comment");
        String postRights = rs.getString("post_rights");
        String xmlBase = rs.getString("xml_base");
        String contributorName = rs.getString("contributor_name");
        String contributorEmail = rs.getString("contributor_email");
        String authorName = rs.getString("author_name");
        String authorEmail = rs.getString("author_email");
        String postCategory = rs.getString("post_category");
        Timestamp publishTimestamp = rs.getTimestamp("publish_timestamp");
        Timestamp expirationTimestamp = rs.getTimestamp("expiration_timestamp");
        String enclosureUrl = rs.getString("enclosure_url");
        Timestamp lastUpdatedTimestamp = rs.getTimestamp("last_updated_timestamp");
        boolean isPublished = rs.getBoolean("is_published");

        StagingPost p = StagingPost.from(
                importerId,
                feedIdent,
                importerId,
                sourceObj,
                sourceName,
                sourceUrl,
                postTitle,
                postDesc,
                postUrl,
                postImgUrl,
                postImgTransportIdent,
                importTimestamp,
                postHash,
                username,
                postComment,
                isPublished,
                postRights,
                xmlBase,
                contributorName,
                contributorEmail,
                authorName,
                authorEmail,
                postCategory,
                publishTimestamp,
                expirationTimestamp,
                enclosureUrl,
                lastUpdatedTimestamp
        );
        p.setId(id);
        if (postStatus != null) {
            try {
                p.setPostStatus(PostStatus.valueOf(postStatus));
            } catch (Exception e) {
                log.error("Unknown post status for postId={}, status={}", p.getId(), postStatus);
            }
        }

        return p;
    };

//    private static final String FIND_PUB_PENDING_SQL = "select * from staging_posts where post_status = 'PUB_PENDING'";

    private static final String FIND_PUB_PENDING_BY_FEED_SQL =
            "select * from staging_posts s " +
            "join feed_definitions f on f.feed_ident = s.feed_ident " +
            "where f.is_active = true and f.username = ? " +
            "and s.post_status = 'PUB_PENDING' " +
            "and s.feed_ident = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> getPubPending(String username, String feedIdent) throws DataAccessException {
        if (isNoneBlank(username, feedIdent)) {
            try {
                return jdbcTemplate.query(FIND_PUB_PENDING_BY_FEED_SQL, new Object[]{username, feedIdent}, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "getPubPending", e.getMessage(), username, feedIdent);
            }
        }

        return Collections.emptyList(); // jdbcTemplate.query(FIND_PUB_PENDING_SQL, STAGING_POST_ROW_MAPPER);
    }

    private static final String FIND_DEPUB_PENDING_BY_FEED_SQL =
            "select * from staging_posts s " +
                    "join feed_definitions f on f.feed_ident = s.feed_ident " +
                    "where f.is_active = true and f.username = ? " +
                    "and s.post_status = 'DEPUB_PENDING' " +
                    "and s.feed_ident = ? and s.username = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> getDepubPending(String username, String feedIdent) throws DataAccessException {
        if (isNotBlank(feedIdent)) {
            try {
                return jdbcTemplate.query(FIND_DEPUB_PENDING_BY_FEED_SQL, new Object[] { username, feedIdent, username }, STAGING_POST_ROW_MAPPER);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "getDepubPending", e.getMessage(), username, feedIdent);
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

    private static final String FIND_BY_USER_SQL = "select * from staging_posts where username = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> findByUser(String username) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_USER_SQL, new Object[] { username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUser", e.getMessage(), username);
        }
    }

    private static final String FIND_BY_USER_AND_FEED_ID_SQL_TEMPLATE = "select * from staging_posts s " +
            "join feed_definitions f on f.feed_ident = s.feed_ident " +
            "join users u on u.name = f.username " +
            "where u.name = ? and f.id in (%s)";

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

    private static final String FIND_ALL_UNPUBLISHED_SQL = "select * from staging_posts where is_published = false";

    @SuppressWarnings("unused")
    List<StagingPost> findAllUnpublished() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_UNPUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAllUnpublished", e.getMessage());
        }
    }

    private static final String FIND_UNPUBLISHED_BY_USER_SQL = "select * from staging_posts where username = ? and is_published = false";

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

    private static final String FIND_ALL_IDLE_SQL_TEMPLATE = "select username,id from staging_posts where (post_status is null or post_status = 'IGNORED') and import_timestamp < current_timestamp - INTERVAL '%s DAYS'";

    @SuppressWarnings("unused")
    Map<String, List<Long>> findAllIdle(int maxAge) throws DataAccessException {
        try {
            String maxAgeStr = Integer.toString(maxAge).replaceAll("[^\\d-]", EMPTY);
            String sql = String.format(FIND_ALL_IDLE_SQL_TEMPLATE, maxAge);
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
            throw new DataAccessException(getClass().getSimpleName(), "findAllIdle", e.getMessage(), maxAge);
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

    private static final String FIND_PUBLISHED_BY_FEED_SQL = "select * from staging_posts " +
            "where is_published = true " +
            "and (post_status is null or post_status != 'DEPUB_PENDING') " +
            "and feed_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public List<StagingPost> findPublishedByFeed(String username, String feedIdent) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_PUBLISHED_BY_FEED_SQL, new Object[] { feedIdent, username }, STAGING_POST_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findPublishedByFeed", e.getMessage(), username, feedIdent);
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
            String sql = String.format(CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE, String.valueOf(id).replaceAll("[^\\d]", ""));
            return jdbcTemplate.queryForObject(sql, new Object[]{username}, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkPublished", e.getMessage(), username, id);
        }
    }

    private static final String MARK_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_status = null, is_published = true where id = ? and username = ?";

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

    private static final String CLEAR_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_status = null, is_published = false where id = ? and username = ?";

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

    private static final String UPDATE_POST_STATUS_BY_ID = "update staging_posts set post_status = ? where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePostStatus(String username, long id, PostStatus postStatus) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_POST_STATUS_BY_ID, postStatus == null ? null : postStatus.toString(), id, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updatePostStatus", e.getMessage(), username, id, postStatus);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePostStatus", username, id, postStatus);
        }
    }

    private static final String UPDATE_POST_BY_ID_TEMPLATE = "update staging_posts set %s where id = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePost(String username, long id, String sourceName, String sourceUrl, String postTitle, String postDesc,
                          String postUrl, String postImgUrl, String postComment, String postRights, String xmlBase,
                          String contributorName, String contributorEmail, String authorName, String authorEmail,
                          String postCategory, Date expirationTimestamp, String enclosureUrl) throws DataAccessException, DataUpdateException {
        List<Object> updateArgs = new ArrayList<>();
        List<String> updateAttrs = new ArrayList<>();

        if (sourceName != null) {
            updateArgs.add(sourceName);
            updateAttrs.add("source_name");
        }
        if (sourceUrl != null) {
            updateArgs.add(sourceUrl);
            updateAttrs.add("source_url");
        }
        if (postTitle != null) {
            updateArgs.add(postTitle);
            updateAttrs.add("post_title");
        }
        if (postDesc != null) {
            updateArgs.add(postDesc);
            updateAttrs.add("post_desc");
        }
        if (postUrl != null) {
            updateArgs.add(postUrl);
            updateAttrs.add("post_url");
        }
        if (postImgUrl != null) {
            //
            updateArgs.add(postImgUrl);
            updateAttrs.add("post_img_url");
            //
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                updateArgs.add(computeThumbnailHash(md, postImgUrl));
                updateAttrs.add("post_img_transport_ident");
            } catch (NoSuchAlgorithmException ignored) {
                // ignored
            }
        }
        if (postComment != null) {
            updateArgs.add(postComment);
            updateAttrs.add("post_comment");
        }
        if (postRights != null) {
            updateArgs.add(postRights);
            updateAttrs.add("post_rights");
        }
        // xml base
        if (xmlBase != null) {
            updateArgs.add(xmlBase);
            updateAttrs.add("xml_base");
        }
        // contributor name
        if (contributorName != null) {
            updateArgs.add(contributorName);
            updateAttrs.add("contributor_name");
        }
        // contributor email
        if (contributorName != null) {
            updateArgs.add(contributorEmail);
            updateAttrs.add("contributor_email");
        }
        // author name
        if (authorName != null) {
            updateArgs.add(authorName);
            updateAttrs.add("author_name");
        }
        // author email
        if (authorEmail != null) {
            updateArgs.add(authorEmail);
            updateAttrs.add("author_email");
        }
        // post category
        if (postCategory != null) {
            updateArgs.add(postCategory);
            updateAttrs.add("post_category");
        }
        // expiration timestamp
        if (expirationTimestamp != null) {
            updateArgs.add(expirationTimestamp);
            updateAttrs.add("expiration_timestamp");
        }
        // last updated timestamp
        updateArgs.add(toTimestamp(new Date()));
        updateAttrs.add("last_updated_timestamp");
        // enclosure URL
        updateArgs.add(id);
        updateArgs.add(username);
        String updateClause = updateAttrs.stream().map(a -> a + "=?").collect(joining(","));
        String updateSql = String.format(UPDATE_POST_BY_ID_TEMPLATE, updateClause);

        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(updateSql, updateArgs.toArray());
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePost attrs=" + updateAttrs, e.getMessage(), updateArgs.toArray());
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePost attrs=" + updateAttrs, updateArgs.toArray());
        }
    }
}
