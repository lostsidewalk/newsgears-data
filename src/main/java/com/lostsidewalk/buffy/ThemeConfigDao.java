package com.lostsidewalk.buffy;


import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Component
@Slf4j
public class ThemeConfigDao {

    private static final Gson GSON = new Gson();

    private static final Type MAP_STRING_TYPE = new TypeToken<Map<String, String>>() {}.getType();

    @Autowired
    private JdbcTemplate jdbcTemplate;

    final RowMapper<ThemeConfig> THEME_CONFIG_ROW_MAPPER = (rs, rowNum) -> {
        // light
        Map<String, String> lightTheme = null;
        String lightThemeConfig = null;
        PGobject lightThemeConfigObj = (PGobject) rs.getObject("light_theme");
        if (lightThemeConfigObj != null) {
            lightThemeConfig = lightThemeConfigObj.getValue();
        }
        if (isNotBlank(lightThemeConfig)) {
            lightTheme = GSON.fromJson(lightThemeConfig, MAP_STRING_TYPE);
        }

        // dark
        Map<String, String> darkTheme = null;
        String darkThemeConfig = null;
        PGobject darkThemeConfigObj = (PGobject) rs.getObject("dark_theme");
        if (darkThemeConfigObj != null) {
            darkThemeConfig = darkThemeConfigObj.getValue();
        }
        if (isNotBlank(darkThemeConfig)) {
            darkTheme = GSON.fromJson(darkThemeConfig, MAP_STRING_TYPE);
        }

        ThemeConfig t = new ThemeConfig();
        t.setDarkTheme(darkTheme);
        t.setLightTheme(lightTheme);
        return t;
    };

    private static final String FIND_BY_USER_ID_SQL = "select * from theme_config where user_id = ?";

    @SuppressWarnings("unused")
    public final ThemeConfig findByUserId(Long userId) throws DataAccessException {
        try {
            List<ThemeConfig> results = jdbcTemplate.query(FIND_BY_USER_ID_SQL, new Object[] { userId }, THEME_CONFIG_ROW_MAPPER);
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUserId", e.getMessage(), userId);
        }
    }

    private static final String UPDATE_THEME_CONFIG_BY_USER_ID_SQL_TEMPLATE = "update theme_config set %s where user_id = ?";

    private static final String INSERT_THEME_CONFIG_BY_USER_ID = "insert into theme_config (user_id,light_theme,dark_theme) values (?,?::json,?::json)";

    @SuppressWarnings("unused")
    public final void upsertThemeConfig(Long userId, Serializable lightTheme, Serializable darkTheme) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        boolean alreadyExists;
        try {
            alreadyExists = checkExists(userId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkExistsByUserId", e.getMessage(), userId);
        }
        if (alreadyExists) {
            // UPDATE PATH
            try {
                List<String> params = new ArrayList<>();
                Object[] args = new Object[2];
                if (lightTheme != null) {
                    params.add("light_theme = ?::json");
                    args[0] = lightTheme;
                }
                if (darkTheme != null) {
                    params.add("dark_theme = ?::json");
                    args[0] = darkTheme;
                }
                args[1] = userId;
                String updateSql = String.format(UPDATE_THEME_CONFIG_BY_USER_ID_SQL_TEMPLATE, Joiner.on(",").join(params));
                rowsUpdated = jdbcTemplate.update(updateSql, args);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateThemeConfig", e.getMessage(), userId, lightTheme, darkTheme);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateThemeConfig", userId, lightTheme, darkTheme);
            }
        } else {
            // INSERT PATH
            try {
                rowsUpdated = jdbcTemplate.update(INSERT_THEME_CONFIG_BY_USER_ID, userId, lightTheme, darkTheme);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "insertThemeConfig", e.getMessage(), userId, lightTheme, darkTheme);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateThemeConfig", userId, lightTheme, darkTheme);
            }
        }
    }

    private static final String CHECK_EXISTS_BY_USER_ID_SQL_TEMPLATE = "select exists(select id from theme_config where user_id = %s)";

    @SuppressWarnings("unused")
    Boolean checkExists(Long userId) throws DataAccessException {
        try {
            NumberFormat nf = NumberFormat.getIntegerInstance();
            nf.setGroupingUsed(false);
            String sql = String.format(CHECK_EXISTS_BY_USER_ID_SQL_TEMPLATE, nf.format(userId));
            return jdbcTemplate.queryForObject(sql, null, Boolean.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), userId);
        }
    }
}
