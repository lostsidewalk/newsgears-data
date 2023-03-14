package com.lostsidewalk.buffy.discovery;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.discovery.FeedDiscoveryInfo.FeedDiscoveryExceptionType;
import com.lostsidewalk.buffy.post.ContentObject;
import com.lostsidewalk.buffy.post.StagingPost;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.sql.Types.INTEGER;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@Slf4j
@Component
public class FeedDiscoveryInfoDao {

    private static final Gson GSON = new Gson();

    private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() {}.getType();

    @Autowired
    JdbcTemplate jdbcTemplate;

    final RowMapper<FeedDiscoveryInfo> FEED_DISCOVERY_INFO_ROW_MAPPER = (rs, rowNum) -> {
        // id
        Long id = rs.getLong("id");
        // feed URL
        String feedUrl = rs.getString("feed_url");
        // http status code
        Integer httpStatusCode = rs.wasNull() ? null : rs.getInt("http_status_code");
        // http status message
        String httpStatusMessage = rs.getString("http_status_message");
        // redirect feed URL
        String redirectFeedUrl = rs.getString("redirect_feed_url");
        // redirect http status code
        Integer redirectHttpStatusCode = rs.wasNull() ? null : rs.getInt("redirect_http_status_code");
        // redirect http status message
        String redirectHttpStatusMessage = rs.getString("redirect_http_status_message");
        // title
        String titleStr = null;
        PGobject titleObj = (PGobject) rs.getObject("title");
        if (titleObj != null) {
            titleStr = titleObj.getValue();
        }
        ContentObject title = GSON.fromJson(titleStr, ContentObject.class);
        // description
        String descriptionStr = null;
        PGobject descriptionObj = (PGobject) rs.getObject("description");
        if (descriptionObj != null) {
            descriptionStr = descriptionObj.getValue();
        }
        ContentObject description = GSON.fromJson(descriptionStr, ContentObject.class);
        // feed_type
        String feedType = rs.getString("feed_type");
        // author
        String author = rs.getString("author");
        // copyright
        String copyright = rs.getString("copyright");
        // docs
        String docs = rs.getString("docs");
        // encoding
        String encoding = rs.getString("encoding");
        // generator
        String generator = rs.getString("generator");
        // image
        String imageStr = null;
        PGobject imageObj = (PGobject) rs.getObject("image");
        if (imageObj != null) {
            imageStr = imageObj.getValue();
        }
        FeedDiscoveryImageInfo image = GSON.fromJson(imageStr, FeedDiscoveryImageInfo.class);
        // icon
        String iconStr = null;
        PGobject iconObj = (PGobject) rs.getObject("icon");
        if (iconObj != null) {
            iconStr = iconObj.getValue();
        }
        FeedDiscoveryImageInfo icon = GSON.fromJson(iconStr, FeedDiscoveryImageInfo.class);
        // language
        String language = rs.getString("language");
        // link
        String link = rs.getString("link");
        // managing_editor
        String managingEditor = rs.getString("managing_editor");
        // published_date
        Timestamp publishedDate = rs.getTimestamp("published_date");
        // supported_types
        String supportedTypesStr = null;
        PGobject supportedTypesObj = (PGobject) rs.getObject("supported_types");
        if (supportedTypesObj != null) {
            supportedTypesStr = supportedTypesObj.getValue();
        }
        ArrayList<String> supportedTypes = GSON.fromJson(supportedTypesStr, LIST_STRING_TYPE);
        // web_master
        String webMaster = rs.getString("web_master");
        // uri
        String uri = rs.getString("uri");
        // categories
        String categoriesStr = null;
        PGobject categoriesObj = (PGobject) rs.getObject("categories");
        if (categoriesObj != null) {
            categoriesStr = categoriesObj.getValue();
        }
        ArrayList<String> categories = GSON.fromJson(categoriesStr, LIST_STRING_TYPE);
        // sample_entries
        String sampleEntriesStr = null;
        PGobject sampleEntriesObj = (PGobject) rs.getObject("sample_entries");
        if (sampleEntriesObj != null) {
            sampleEntriesStr = sampleEntriesObj.getValue();
        }
        ArrayList<StagingPost> sampleEntries = GSON.fromJson(sampleEntriesStr, new TypeToken<List<StagingPost>>() {}.getType());
        // is_url_upgradeable
        boolean isUrlUpgradable = rs.getBoolean("is_url_upgradeable");
        // error type
        String errorType = rs.getString("error_type");
        // error detail
        String errorDetail = rs.getString("error_detail");

        FeedDiscoveryInfo i = FeedDiscoveryInfo.from(
                feedUrl,
                httpStatusCode,
                httpStatusMessage,
                redirectFeedUrl,
                redirectHttpStatusCode,
                redirectHttpStatusMessage,
                title,
                description,
                feedType,
                author,
                copyright,
                docs,
                encoding,
                generator,
                image,
                icon,
                language,
                link,
                managingEditor,
                publishedDate,
                EMPTY, // stylesheet
                supportedTypes,
                webMaster,
                uri,
                categories,
                sampleEntries,
                isUrlUpgradable
        );
        i.setId(id);
        i.setErrorType(ofNullable(errorType).map(e -> FeedDiscoveryExceptionType.valueOf(errorType)).orElse(null)); // TODO: safety
        i.setErrorDetail(errorDetail);

        return i;
    };

    private static final String FIND_ALL_SQL = "select * from feed_discovery_info";

    @SuppressWarnings("unused")
    public List<FeedDiscoveryInfo> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, FEED_DISCOVERY_INFO_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String FIND_DISCOVERABLE_SQL = "select * from feed_discovery_info where error_type is null";

    @SuppressWarnings("unused")
    public List<FeedDiscoveryInfo> findDiscoverable() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_DISCOVERABLE_SQL, FEED_DISCOVERY_INFO_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }

    private static final String UPDATE_FEED_DISCOVERY_INFO_SQL = "update feed_discovery_info set " +
            "feed_url = ?, " +
            "http_status_code = ?, " +
            "http_status_message = ?, " +
            "redirect_feed_url = ?, " +
            "redirect_http_status_code = ?, " +
            "redirect_http_status_message = ?, " +
            "title = ?::json, " +
            "description = ?::json, " +
            "feed_type = ?, " +
            "author = ?, " +
            "copyright = ?, " +
            "docs = ?, " +
            "encoding = ?, " +
            "generator = ?, " +
            "image = ?::json, " +
            "icon = ?::json, " +
            "language = ?, " +
            "link = ?, " +
            "managing_editor = ?, " +
            "published_date = ?, " +
            "supported_types = ?::json, " +
            "web_master = ?, " +
            "uri = ?, " +
            "categories = ?::json, " +
            "sample_entries = ?::json, " +
            "is_url_upgradeable = ?, " +
            "error_type = ?, " +
            "error_detail = ? " +
        "where id = ?";

    @SuppressWarnings("unused")
    public void update(FeedDiscoveryInfo feedDiscoveryInfo) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_FEED_DISCOVERY_INFO_SQL, ps -> {
                // feed_url
                ps.setString(1, feedDiscoveryInfo.getFeedUrl());
                // http status code
                if (feedDiscoveryInfo.getHttpStatusCode() == null) {
                    ps.setNull(2, INTEGER);
                } else {
                    ps.setInt(2, feedDiscoveryInfo.getHttpStatusCode());
                }
                // http status message
                ps.setString(3, feedDiscoveryInfo.getHttpStatusMessage());
                // redirect feed URL
                ps.setString(4, feedDiscoveryInfo.getRedirectFeedUrl());
                // redirect http status code
                if (feedDiscoveryInfo.getRedirectHttpStatusCode() == null) {
                    ps.setNull(5, INTEGER);
                } else {
                    ps.setInt(5, feedDiscoveryInfo.getRedirectHttpStatusCode());
                }
                // redirect http status message
                ps.setString(6, feedDiscoveryInfo.getRedirectHttpStatusMessage());
                //
                ps.setString(7, GSON.toJson(feedDiscoveryInfo.getTitle()));
                ps.setString(8, GSON.toJson(feedDiscoveryInfo.getDescription()));
                ps.setString(9, feedDiscoveryInfo.getFeedType());
                ps.setString(10, feedDiscoveryInfo.getAuthor());
                ps.setString(11, feedDiscoveryInfo.getCopyright());
                ps.setString(12, feedDiscoveryInfo.getDocs());
                ps.setString(13, feedDiscoveryInfo.getEncoding());
                ps.setString(14, feedDiscoveryInfo.getGenerator());
                ps.setString(15, GSON.toJson(feedDiscoveryInfo.getImage()));
                ps.setString(16, GSON.toJson(feedDiscoveryInfo.getIcon()));
                ps.setString(17, feedDiscoveryInfo.getLanguage());
                ps.setString(18, feedDiscoveryInfo.getLink());
                ps.setString(19, feedDiscoveryInfo.getManagingEditor());
                ps.setTimestamp(20, toTimestamp(feedDiscoveryInfo.getPublishedDate()));
                ps.setString(21, GSON.toJson(feedDiscoveryInfo.getSupportedTypes()));
                ps.setString(22, feedDiscoveryInfo.getWebMaster());
                ps.setString(23, feedDiscoveryInfo.getUri());
                ps.setString(24, GSON.toJson(feedDiscoveryInfo.getCategories()));
                ps.setString(25, GSON.toJson(feedDiscoveryInfo.getSampleEntries()));
                ps.setBoolean(26, feedDiscoveryInfo.isUrlUpgradable());
                ps.setString(27, ofNullable(feedDiscoveryInfo.getErrorType()).map(Enum::name).orElse(null));
                ps.setString(28, feedDiscoveryInfo.getErrorDetail());
                ps.setLong(29, feedDiscoveryInfo.getId());
            });
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "update", e.getMessage(), feedDiscoveryInfo);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "update", feedDiscoveryInfo);
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
