package com.lostsidewalk.buffy.auth;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Data access object for managing feature definitions in the application.
 */
@Slf4j
@Component
public class FeatureDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final RowMapper<String> mapper = (rs, rowNum) -> rs.getString("feature_cd");

    /**
     * Retrieves a list of feature codes associated with a specified role name.
     *
     * @param rolename The name of the role for which to retrieve associated feature codes.
     * @return A list of feature codes associated with the specified role name.
     * @throws DataAccessException If there is an issue accessing the data.
     */
    @SuppressWarnings("unused")
    public List<String> findByRolename(String rolename) throws DataAccessException {
        String findByRoleNameSql = "select feature_cd from features_in_roles fir "
                + " join roles r on r.name = fir.role "
                + " where r.name = ?";
        try {
            return jdbcTemplate.query(findByRoleNameSql, mapper, rolename);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByRoleName", e.getMessage(), rolename);
        }
    }
}
