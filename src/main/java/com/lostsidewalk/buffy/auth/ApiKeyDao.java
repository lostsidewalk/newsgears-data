package com.lostsidewalk.buffy.auth;

import com.google.common.collect.ImmutableList;
import com.lostsidewalk.buffy.AbstractDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARCHAR;

/**
 * Data access object for managing API keys in the application.
 */
@Slf4j
@Component
public class ApiKeyDao extends AbstractDao<ApiKey> {

    private String findByUserIdSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("${newsgears.data.apikeys.table}")
    String tableName;

    /**
     * Default constructor; initializes the object.
     */
    ApiKeyDao() {
    }

    @Override
    protected final void setupSQL() {
        findByUserIdSQL = String.format("select * from %s where user_id = ? and application_id = '%s'", tableName, getApplicationId());
    }

    private static final String NAME_ATTRIBUTE = "api_key";

    @SuppressWarnings("MethodReturnAlwaysConstant")
    @Override
    protected final String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = ImmutableList.of(
            "user_id",
            "api_key",
            "api_secret"
    );

    @Override
    protected final List<String> getInsertAttributes() {
        return INSERT_ATTRIBUTES;
    }

    @Override
    protected final void configureInsertParams(MapSqlParameterSource parameters, ApiKey entity) {
        configureCommonParams(parameters, entity);
    }

    @Override
    protected final void setId(ApiKey entity, long id) {
        entity.setId(id);
    }

    @Override
    protected final void configureUpdateParams(MapSqlParameterSource parameters, ApiKey entity) {
        configureCommonParams(parameters, entity);
        parameters.addValue("id", entity.getId(), NUMERIC);
    }

    private static void configureCommonParams(MapSqlParameterSource parameters, ApiKey apiKey) {
        parameters.addValue("user_id", apiKey.getUserId(), NUMERIC);
        parameters.addValue("api_key", apiKey.getApiKey(), VARCHAR);
        parameters.addValue("api_secret", apiKey.getApiSecret(), VARCHAR);
    }

    @Override
    protected final JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    private static final RowMapper<ApiKey> API_KEY_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");

        Long userId = rs.getLong("user_id");

        String apiKey = rs.getString("api_key");

        String apiSecret = rs.getString("api_secret");

        return ApiKey.from(id, userId, apiKey, apiSecret);
    };

    @Override
    protected final RowMapper<ApiKey> getRowMapper() {
        return API_KEY_ROW_MAPPER;
    }

    @Override
    protected final String getTableName() {
        return tableName;
    }

    @Override
    protected final String getDescription() {
        return "findByUserIdSQL='" + findByUserIdSQL + '\'' +
                ", jdbcTemplate=" + jdbcTemplate +
                ", tableName='" + tableName + '\'';
    }

    /**
     * Retrieves an API key associated with the given user ID.
     *
     * @param userId The ID of the user for which to retrieve the API key.
     * @return An `ApiKey` object representing the API key associated with the user, or `null` if the user ID is `null`.
     */
    @SuppressWarnings("unused")
    public final ApiKey findByUserId(Long userId) {
        if (null != userId) {
            return jdbcTemplate.queryForObject(findByUserIdSQL, API_KEY_ROW_MAPPER, userId);
        }

        return null;
    }
}
