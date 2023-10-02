package com.lostsidewalk.buffy.thumbnail;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Data access object for managing thumbnail data in the application.
 */
@Slf4j
@Component
public class ThumbnailDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    /**
     * Default constructor; initializes the object.
     */
    ThumbnailDao() {
        super();
    }

    private static final String FIND_ALL_SQL = "select img_src from thumbnails";

    final RowMapper<String> THUMBNAIL_ROW_MAPPER = (rs, rowNum) -> rs.getString("img_src");

    /**
     * Retrieves a list of all thumbnail image source URLs from the database.
     *
     * @return A List of String containing the image source URLs of all thumbnails.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<String> findAll() throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_ALL_SQL, THUMBNAIL_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findAll", e.getMessage());
        }
    }
}
