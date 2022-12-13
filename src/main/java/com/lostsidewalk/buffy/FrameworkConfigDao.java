package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lostsidewalk.buffy.FrameworkConfig.NOTIFICATION_CONFIG;
import static java.util.Arrays.stream;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

@Component
@Slf4j
public class FrameworkConfigDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SELECT_BY_USER_ID_SQL = "select settings_group,attr_name,attr_value from framework_config where user_id = ?";

    private static final String DELETE_SETTINGS_GROUP_BY_USER_ID_SQL = "delete from framework_config where user_id = ? and settings_group = ?";

    private static final String INSERT_SQL = "insert into framework_config (user_id, settings_group, attr_name, attr_value) values (?,?,?,?)";

    @SuppressWarnings("unused")
    public final FrameworkConfig findByUserId(Long userId) throws DataAccessException {
        try {
            //
            FrameworkConfig frameworkConfig = new FrameworkConfig();
            frameworkConfig.setUserId(userId);
            //
            PreparedStatement ps = null;
            ResultSet rs = null;
            DataSource dataSource = jdbcTemplate.getDataSource();
            if (dataSource != null) {
                Connection conn = null;
                try {
                    conn = dataSource.getConnection();
                    ps = conn.prepareStatement(SELECT_BY_USER_ID_SQL);
                    ps.setLong(1, userId);
                    rs = ps.executeQuery();
                    Map<String, String> notificationConfig = new HashMap<>();
                    while (rs.next()) {
                        String _settingsGroup = rs.getString("settings_group");
                        String _attrName = rs.getString("attr_name");
                        String _attrValue = rs.getString("attr_value");
                        Map<String, String> m = null;
                        //noinspection SwitchStatementWithTooFewBranches
                        switch (_settingsGroup) {
                            case NOTIFICATION_CONFIG -> m = notificationConfig;
                        }
                        if (m != null) {
                            m.put(_attrName, _attrValue);
                        }
                    }
                    frameworkConfig.setNotifications(notificationConfig);
                } finally {
                    closeQuietly(ps);
                    closeQuietly(rs);
                    closeQuietly(conn);
                }
            }

            return frameworkConfig;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByUserId", e.getMessage(), userId);
        }
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

    @SuppressWarnings("unused")
    public final void save(FrameworkConfig frameworkConfig) throws DataAccessException, DataUpdateException {
        int rowsAffected;
        try {
            Long userId = frameworkConfig.getUserId();
            // insert the notification config; remove entries w/null values first
            rowsAffected = doSettingsGroup(userId, NOTIFICATION_CONFIG, cleanSettingsGroup(frameworkConfig.getNotifications()));
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "save", e.getMessage(), frameworkConfig);
        }
        if (!(rowsAffected > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "save", frameworkConfig);
        }
    }

    private Map<String, String> cleanSettingsGroup(Map<String, String> settingsGroup) {
        settingsGroup.values().removeAll(singleton(null));
        return settingsGroup;
    }

    private int doSettingsGroup(Long userId, @SuppressWarnings("SameParameterValue") String settingsGroup, Map<String, String> attributes) {
        // drop the existing user settings
        clearSettingsGroup(userId, settingsGroup);
        List<Object[]> batchArgs = attributes.entrySet().stream().map(e -> new Object[] {
            userId, settingsGroup, e.getKey(), e.getValue()
        }).collect(toList());
        return stream(this.jdbcTemplate.batchUpdate(INSERT_SQL, batchArgs)).sum();
    }

    private void clearSettingsGroup(Long userId, String settingsGroup) {
        jdbcTemplate.update(DELETE_SETTINGS_GROUP_BY_USER_ID_SQL, userId, settingsGroup);
    }
}
