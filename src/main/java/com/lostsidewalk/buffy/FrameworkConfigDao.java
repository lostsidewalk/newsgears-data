package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.lostsidewalk.buffy.FrameworkConfig.DISPLAY_CONFIG;
import static com.lostsidewalk.buffy.FrameworkConfig.NOTIFICATION_CONFIG;
import static java.util.Arrays.stream;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;

/**
 * Data access object for managing framework configuration data in the application.
 */
@SuppressWarnings("OverlyBroadCatchBlock")
@Component
@Slf4j
public class FrameworkConfigDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    FrameworkConfigDao() {
    }

    private static final String SELECT_BY_USER_ID_SQL = "select settings_group,attr_name,attr_value from framework_config where user_id = ?";

    private static final String DELETE_SETTINGS_GROUP_BY_USER_ID_SQL = "delete from framework_config where user_id = ? and settings_group = ?";

    private static final String INSERT_SQL = "insert into framework_config (user_id, settings_group, attr_name, attr_value) values (?,?,?,?)";

    /**
     * Retrieves a FrameworkConfig object associated with a specified user ID.
     *
     * @param userId The ID of the user for which to retrieve the FrameworkConfig.
     * @return A FrameworkConfig object containing user-specific configuration settings.
     * @throws DataAccessException If there is an issue accessing the data.
     */
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
            if (null != dataSource) {
                Connection conn = null;
                try {
                    conn = dataSource.getConnection();
                    ps = conn.prepareStatement(SELECT_BY_USER_ID_SQL);
                    ps.setLong(1, userId);
                    rs = ps.executeQuery();
                    Map<String, String> notificationConfig = new HashMap<>(16);
                    Map<String, String> displayConfig = new HashMap<>(16);
                    while (rs.next()) {
                        String _settingsGroup = rs.getString("settings_group");
                        String _attrName = rs.getString("attr_name");
                        String _attrValue = rs.getString("attr_value");
                        Map<String, String> map = null;
                        //noinspection SwitchStatement
                        switch (_settingsGroup) {
                            case NOTIFICATION_CONFIG -> map = notificationConfig;
                            case DISPLAY_CONFIG -> map = displayConfig;
                        }
                        if (null != map) {
                            map.put(_attrName, _attrValue);
                        }
                    }
                    frameworkConfig.setNotifications(notificationConfig);
                    frameworkConfig.setDisplay(displayConfig);
                } finally {
                    closeQuietly(ps);
                    closeQuietly(rs);
                    closeQuietly(conn);
                }
            }

            return frameworkConfig;
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByUserId", e.getMessage(), userId);
        }
    }

    private static void closeQuietly(Connection conn) {
        if (null == conn) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ignored) {}
    }

    private static void closeQuietly(Statement ps) {
        if (null == ps) {
            return;
        }
        try {
            ps.close();
        } catch (SQLException ignored) {}
    }

    private static void closeQuietly(ResultSet rs) {
        if (null == rs) {
            return;
        }
        try {
            rs.close();
        } catch (SQLException ignored) {}
    }

    /**
     * Saves a FrameworkConfig object, updating user-specific configuration settings in the database.
     *
     * @param frameworkConfig The FrameworkConfig object containing user-specific configuration settings to save.
     * @throws DataAccessException  If there is an issue accessing the data.
     * @throws DataUpdateException If there is an issue updating the data.
     * @throws DataConflictException If a data conflict occurs during the save operation.
     */
    @SuppressWarnings("unused")
    public final void save(FrameworkConfig frameworkConfig) throws DataAccessException, DataUpdateException, DataConflictException {
        int rowsAffected;
        try {
            Long userId = frameworkConfig.getUserId();
            // insert the notification config; remove entries w/null values first
            rowsAffected = doSettingsGroup(userId, NOTIFICATION_CONFIG, cleanSettingsGroup(frameworkConfig.getNotifications()));
            rowsAffected += doSettingsGroup(userId, DISPLAY_CONFIG, cleanSettingsGroup(frameworkConfig.getDisplay()));
        } catch (DuplicateKeyException e) {
            throw new DataConflictException(getClass().getSimpleName(), "update", e.getMessage(), frameworkConfig);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "save", e.getMessage(), frameworkConfig);
        }
        if (!(0 < rowsAffected)) {
            throw new DataUpdateException(getClass().getSimpleName(), "save", frameworkConfig);
        }
    }

    private static Map<String, String> cleanSettingsGroup(Map<String, String> settingsGroup) {
        settingsGroup.values().removeAll(singleton(null));
        return settingsGroup;
    }

    private int doSettingsGroup(Long userId, @SuppressWarnings("SameParameterValue") String settingsGroup, Map<String, String> attributes) {
        // drop the existing user settings
        clearSettingsGroup(userId, settingsGroup);
        List<Object[]> batchArgs = attributes.entrySet().stream().map(e -> new Object[] {
            userId, settingsGroup, e.getKey(), e.getValue()
        }).collect(toList());
        return stream(jdbcTemplate.batchUpdate(INSERT_SQL, batchArgs)).sum();
    }

    private void clearSettingsGroup(Long userId, String settingsGroup) {
        jdbcTemplate.update(DELETE_SETTINGS_GROUP_BY_USER_ID_SQL, userId, settingsGroup);
    }

    @Override
    public final String toString() {
        return "FrameworkConfigDao{" +
                "jdbcTemplate=" + jdbcTemplate +
                '}';
    }
}
