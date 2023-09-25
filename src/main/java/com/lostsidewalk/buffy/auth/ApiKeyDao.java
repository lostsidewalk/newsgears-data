package com.lostsidewalk.buffy.auth;

import com.lostsidewalk.buffy.AbstractDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARCHAR;

/**
 * Data access object for managing API keys in the application.
 */
@Component
public class ApiKeyDao extends AbstractDao<ApiKey> {

    private String findByUserIdSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("${newsgears.data.apikeys.table}")
    String tableName;

    @Override
    protected void setupSQL() {
        this.findByUserIdSQL = String.format("select * from %s where user_id = ? and application_id = '%s'", getTableName(), this.applicationId);
    }

    private static final String NAME_ATTRIBUTE = "api_key";

    @Override
    protected String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = newArrayList(
            "user_id",
            "api_key",
            "api_secret"
    );

    @Override
    protected List<String> getInsertAttributes() {
        return INSERT_ATTRIBUTES;
    }

    @Override
    protected void configureInsertParams(MapSqlParameterSource parameters, ApiKey entity) {
        configureCommonParams(parameters, entity);
    }

    @Override
    protected void configureUpdateParams(MapSqlParameterSource parameters, ApiKey entity) {
        configureCommonParams(parameters, entity);
        parameters.addValue("id", entity.getId(), NUMERIC);
    }

    private void configureCommonParams(MapSqlParameterSource parameters, ApiKey apiKey) {
        parameters.addValue("user_id", apiKey.getUserId(), NUMERIC);
        parameters.addValue("api_key", apiKey.getApiKey(), VARCHAR);
        parameters.addValue("api_secret", apiKey.getApiSecret(), VARCHAR);
    }

    @Override
    protected JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    static final RowMapper<ApiKey> API_KEY_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");

        Long userId = rs.getLong("user_id");

        String apiKey = rs.getString("api_key");

        String apiSecret = rs.getString("api_secret");

        return ApiKey.from(id, userId, apiKey, apiSecret);
    };

    @Override
    protected RowMapper<ApiKey> getRowMapper() {
        return API_KEY_ROW_MAPPER;
    }

    @Override
    protected String getTableName() {
        return tableName;
    }

    /**
     * Retrieves an API key associated with the given user ID.
     *
     * @param userId The ID of the user for which to retrieve the API key.
     * @return An `ApiKey` object representing the API key associated with the user, or `null` if the user ID is `null`.
     */
    @SuppressWarnings("unused")
    public ApiKey findByUserId(Long userId) {
        if (userId != null) {
            return getJdbcTemplate().queryForObject(this.findByUserIdSQL, getRowMapper(), userId);
        }

        return null;
    }
}
