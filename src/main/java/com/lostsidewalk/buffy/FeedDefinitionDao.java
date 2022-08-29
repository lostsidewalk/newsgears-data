package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
class FeedDefinitionDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String CHECK_EXISTS_BY_ID_SQL_TEMPLATE = "select exists(select id from feed_definitions where id = '%s')";

    @SuppressWarnings("unused")
    Boolean checkExists(String id) {
        String sql = String.format(CHECK_EXISTS_BY_ID_SQL_TEMPLATE, id);
        return jdbcTemplate.queryForObject(sql, null, Boolean.class);
    }

    private static final String INSERT_FEED_DEFINITIONS_SQL =
            "insert into feed_definitions (" +
                    "feed_ident," +
                    "feed_title," +
                    "feed_desc," +
                    "feed_generator," +
                    "transport_ident" +
                    ") values " +
                    "(?,?,?,?,?)";

    @SuppressWarnings("unused")
    void add(FeedDefinition feedDefinition) {
        int rowsUpdated = jdbcTemplate.update(INSERT_FEED_DEFINITIONS_SQL,
                feedDefinition.getIdent(),
                feedDefinition.getTitle(),
                feedDefinition.getDescription(),
                feedDefinition.getGenerator(),
                feedDefinition.getTransportIdent()
        );
        log.debug("Updating {} rows", rowsUpdated);
    }

    RowMapper<FeedDefinition> FEED_DEFINITION_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        String feedIdent = rs.getString("feed_ident");
        String feedTitle = rs.getString("feed_title");
        String feedDesc = rs.getString("feed_desc");
        String feedGenerator = rs.getString("feed_generator");
        String transportIdent = rs.getString("transport_ident");

        FeedDefinition f = new FeedDefinition(
                feedIdent,
                feedTitle,
                feedDesc,
                feedGenerator,
                transportIdent
        );
        f.setId(id);

        return f;
    };

    private static final String DELETE_BY_ID_SQL = "delete from feed_definitions where id = ?";

    @SuppressWarnings("unused")
    int deleteById(long id) {
        return jdbcTemplate.update(DELETE_BY_ID_SQL, id);
    }

    private static final String FIND_ALL_SQL = "select * from feed_definitions";

    @SuppressWarnings("unused")
    List<FeedDefinition> findAll() {
        return jdbcTemplate.query(FIND_ALL_SQL, FEED_DEFINITION_ROW_MAPPER);
    }

    private static final String FIND_BY_FEED_IDENT_SQL = "select * from feed_definitions where feed_ident = ?";

    @SuppressWarnings("unused")
    FeedDefinition findByFeedIdent(String feedIdent) {
        List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_FEED_IDENT_SQL, new Object[] { feedIdent }, FEED_DEFINITION_ROW_MAPPER);
        return results.isEmpty() ? null : results.get(0);
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select * from feed_definitions where transport_ident = ?";

    @SuppressWarnings("unused")
    FeedDefinition findByTransportIdent(String transportIdent) {
        List<FeedDefinition> results = jdbcTemplate.query(FIND_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent }, FEED_DEFINITION_ROW_MAPPER);
        return results.isEmpty() ? null : results.get(0);
    }
}
