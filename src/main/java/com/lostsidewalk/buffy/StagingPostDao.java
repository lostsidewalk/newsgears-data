package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.StagingPost.PostStatus;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

import static com.lostsidewalk.buffy.StagingPost.PostStatus.PUB_PENDING;
import static com.lostsidewalk.buffy.StagingPost.PostStatus.valueOf;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@Component
class StagingPostDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_HASH_SQL_TEMPLATE = "select exists(select id from staging_posts where post_hash = '%s')";

    @SuppressWarnings("unused")
    Boolean checkExists(String stagingPostHash) {
        String sql = String.format(CHECK_EXISTS_BY_HASH_SQL_TEMPLATE, stagingPostHash);
        return jdbcTemplate.queryForObject(sql, null, Boolean.class);
    }

    private static final String INSERT_STAGING_POST_SQL =
            "insert into staging_posts (" +
                    "post_hash," +
                    "post_title," +
                    "post_desc," +
                    "post_url," +
                    "post_img_url," +
                    "importer_id," +
                    "tag_name," +
                    "object_source," +
                    "import_timestamp," +
                    "post_status" +
                    ") values " +
                    "(?,?,?,?,?,?,?,cast(? as json),?,?)";

    @SuppressWarnings("unused")
    void add(StagingPost stagingPost) {
        int rowsUpdated = jdbcTemplate.update(INSERT_STAGING_POST_SQL,
                stagingPost.getPostHash(),
                stagingPost.getPostTitle(),
                stagingPost.getPostDesc(),
                stagingPost.getPostUrl(),
                stagingPost.getPostImgUrl(),
                stagingPost.getImporterId(),
                stagingPost.getTagName(),
                stagingPost.getSourceObj().toString(),
                stagingPost.getImportTimestamp(),
                stagingPost.getPostStatus()
        );
        log.debug("Updating {} rows", rowsUpdated);
    }

    RowMapper<StagingPost> STAGING_POST_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String postTitle = rs.getString("post_title");
        String postDesc = rs.getString("post_desc");
        String postUrl = rs.getString("post_url");
        String postImgUrl = rs.getString("post_img_url");
        String importerId = rs.getString("importer_id");
        String tagName = rs.getString("tag_name");
        String sourceObj = ((PGobject) rs.getObject("object_source")).getValue();
        Date importTimestamp = rs.getTimestamp("import_timestamp");
        String postStatus = rs.getString("post_status");
        String postHash = rs.getString("post_hash");

        StagingPost p = new StagingPost(
                importerId,
                tagName,
                importerId, // TODO: add importerDesc
                postTitle,
                postDesc,
                postUrl,
                postImgUrl,
                sourceObj,
                importTimestamp,
                postHash
        );
        p.setId(id);
        if (postStatus != null) {
            try {
                p.setPostStatus(valueOf(postStatus));
            } catch (Exception e) {
                log.error("Unknown post status for postId={}, status={}", p.getId(), postStatus);
            }
        }

        return p;
    };

    private static final String FIND_PUB_PENDING_SQL = "select * from staging_posts where post_status = 'PUB_PENDING'";

    private static final String FIND_PUB_PENDING_BY_TAG_SQL = "select * from staging_posts where post_status = 'PUB_PENDING' and tag_name = ?";

    @SuppressWarnings("unused")
    List<StagingPost> getPubPending(String tag) {
        if (isNotBlank(tag)) {
            return jdbcTemplate.query(FIND_PUB_PENDING_BY_TAG_SQL, new Object[] { tag }, STAGING_POST_ROW_MAPPER);
        }

        return jdbcTemplate.query(FIND_PUB_PENDING_SQL, STAGING_POST_ROW_MAPPER);
    }

    private static final String DELETE_BY_ID_SQL = "delete from staging_posts where id = ?";

    @SuppressWarnings("unused")
    int deleteById(long id) {
        return jdbcTemplate.update(DELETE_BY_ID_SQL, id);
    }

    private static final String FIND_ALL_SQL = "select * from staging_posts";

    @SuppressWarnings("unused")
    List<StagingPost> findAll() {
        return jdbcTemplate.query(FIND_ALL_SQL, STAGING_POST_ROW_MAPPER);
    }

    private static final String FIND_ALL_UNPUBLISHED_SQL = "select * from staging_posts where is_published = false";

    @SuppressWarnings("unused")
    List<StagingPost> findAllUnpublished() {
        return jdbcTemplate.query(FIND_ALL_UNPUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
    }

    private static final String FIND_ALL_PUBLISHED_SQL = "select * from staging_posts where is_published = true";

    @SuppressWarnings("unused")
    List<StagingPost> findAllPublished() {
        return jdbcTemplate.query(FIND_ALL_PUBLISHED_SQL, STAGING_POST_ROW_MAPPER);
    }

    private static final String CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE = "select is_published from staging_posts where id = %s";

    @SuppressWarnings("unused")
    Boolean checkPublished(long id) {
        String sql = String.format(CHECK_PUBLISHED_BY_ID_SQL_TEMPLATE, String.valueOf(id).replaceAll("[^\\d]", ""));
        return jdbcTemplate.queryForObject(sql, null, Boolean.class);
    }

    private static final String MARK_PUB_PENDING_BY_ID_SQL_TEMPLATE = "update staging_posts set post_status = %s where id = ?";

    @SuppressWarnings("unused")
    int markPubPending(long id, boolean publish) {
        PostStatus postStatus = publish ? PUB_PENDING : null;
        String sql = String.format(MARK_PUB_PENDING_BY_ID_SQL_TEMPLATE, postStatus == null ? null : addSingleQuotes(postStatus.toString()));
        return jdbcTemplate.update(sql, id);
    }

    private static final String MARK_PUB_COMPLETE_BY_ID_SQL = "update staging_posts set post_status = null, is_published = true where id = ?";

    @SuppressWarnings("unused")
    int markPubComplete(long id) {
        return jdbcTemplate.update(MARK_PUB_COMPLETE_BY_ID_SQL, id);
    }
    //
    // utility methods
    //
    private static String addSingleQuotes(String s) {
        return "'" + s + "'";
    }
}
