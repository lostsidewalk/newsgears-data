package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.lostsidewalk.buffy.FrameworkConfig.FEED_CONFIG;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Types.VARCHAR;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;

@Component
@Slf4j
public class FrameworkConfigDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    static class Setting {
        final String username;
        final String settingsGroup;
        final String attrName;
        final String attrValue;

        Setting(String username, String settingsGroup, String attrName, String attrValue) {
            this.username = username;
            this.settingsGroup = settingsGroup;
            this.attrName = attrName;
            this.attrValue = attrValue;
        }
    }

    private static final String SELECT_SQL = "select username,settings_group,attr_name,attr_value from framework_config where username = ?";

    private static final String DELETE_SQL = "delete from framework_config where username = ?";

    private static final String UPSERT_SQL = "insert into framework_config (username, settings_group, attr_name, attr_value) " +
            "values (:username, :settingsGroup, :attrName, :attrValue) " +
            "on conflict " +
            "on constraint framework_config_username_settings_group_attr_name_key " +
            "do update set attr_value = :attrValue";

    private static final String INSERT_SQL = "insert into framework_config (username, settings_group, attr_name, attr_value) " +
            "values (:username, :settingsGroup, :attrName, :attrValue) ";

    public final FrameworkConfig findByUsername(String username) {
        //
        FrameworkConfig frameworkConfig = new FrameworkConfig();
        frameworkConfig.setUsername(username);
        //
        PreparedStatement ps = null;
        ResultSet rs = null;
        DataSource dataSource = jdbcTemplate.getDataSource();
        if (dataSource != null) {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                ps = conn.prepareStatement(SELECT_SQL);
                ps.setString(1, username);
                rs = ps.executeQuery();
                Map<String, String> feedConfig = new HashMap<>();
                while (rs.next()) {
                    String _settingsGroup = rs.getString("settings_group");
                    String _attrName = rs.getString("attr_name");
                    String _attrValue = rs.getString("attr_value");
                    Map<String, String> m = null;
                    switch (_settingsGroup) {
                        case FEED_CONFIG: {
                            m = feedConfig;
                            break;
                        }
                    }
                    if (m != null) {
                        m.put(_attrName, _attrValue);
                    }
                }
                frameworkConfig.setFeed(feedConfig);
            } catch (SQLException e) {
                log.error(getRootCauseMessage(e), e);
                throw new RuntimeException(e);
                // TODO: do something here
            } finally {
                closeQuietly(ps);
                closeQuietly(rs);
                closeQuietly(conn);
            }
        }

        return frameworkConfig;
    }

    private static void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ignored) {}
    }

    private static void closeQuietly(PreparedStatement ps) {
        if (ps == null) {
            return;
        }
        try {
            ps.close();
        } catch (SQLException ignored) {}
    }

    private static void closeQuietly(ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (SQLException ignored) {}
    }

    public final void upsert(FrameworkConfig frameworkConfig) {
        @SuppressWarnings("unused") int rowsAffected = 0;

        String username = frameworkConfig.getUsername();
        rowsAffected += clearAllSettings(username);
        //noinspection UnusedAssignment
        // TODO: log rows affected or refactor it away
        rowsAffected += doSettingsGroup(username, FEED_CONFIG, frameworkConfig.getFeed(), this::execUpsert);
    }

    private int clearAllSettings(String username) {
        return jdbcTemplate.update(DELETE_SQL, username);
    }

    private int doSettingsGroup(String username, String settingsGroup, Map<String, String> attributes, Function<MapSqlParameterSource, Integer> cons) {
        int rowsAffected = 0;
        for (Map.Entry<String, String> setting : attributes.entrySet()) {
            String attrName = setting.getKey();
            String attrValue = setting.getValue();
            MapSqlParameterSource parameters = new MapSqlParameterSource();
            configureParams(parameters, username, settingsGroup, attrName, attrValue);
            rowsAffected += cons.apply(parameters);
        }

        return rowsAffected;
    }

    private void configureParams(MapSqlParameterSource parameters, String username, String settingsGroup, String attrName, String attrValue) {
        parameters.addValue("username", username, VARCHAR);
        parameters.addValue("attrValue", attrValue, VARCHAR);
        parameters.addValue("settingsGroup", settingsGroup, VARCHAR);
        parameters.addValue("attrName", attrName, VARCHAR);
    }

    private int execUpsert(MapSqlParameterSource parameters) {
        return exec(parameters, UPSERT_SQL);
    }

    private int execInsert(MapSqlParameterSource parameters) {
        return exec(parameters, INSERT_SQL);
    }

    private int exec(MapSqlParameterSource parameters, String sql) {
        DataSource dataSource = jdbcTemplate.getDataSource();
        if (dataSource != null) {
            NamedParameterStatement ps = null;
            try (Connection conn = dataSource.getConnection()) {
                ps = new NamedParameterStatement(conn, sql, NO_GENERATED_KEYS);
                for (String p : parameters.getParameterNames()) {
                    Object v = parameters.getValue(p);
                    ps.setObject(p, v, parameters.getSqlType(p));
                }
                return ps.executeUpdate();
            } catch (SQLException e) {
                log.error(getRootCauseMessage(e), e); // TODO: throw something here
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (Exception ignored) {}
                }
            }
        }

        return 0;
    }

    private static final String DELETE_BY_USERNAME_SQL = "delete from framework_config where username = ?";

    public void deleteByUsername(String username) {
        if (isNotBlank(username)) {
            Object[] args = new Object[] {username};
            this.jdbcTemplate.update(DELETE_BY_USERNAME_SQL, args);
        }
    }
}
