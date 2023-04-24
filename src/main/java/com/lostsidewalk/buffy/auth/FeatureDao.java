package com.lostsidewalk.buffy.auth;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class FeatureDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final RowMapper<String> mapper = (rs, rowNum) -> rs.getString("feature_cd");

    @SuppressWarnings("unused")
    public List<String> findByRolename(String rolename) throws DataAccessException {
        String findByRoleNameSql = "select feature_cd from features_in_roles fir "
                + " join roles r on r.name = fir.role "
                + " where r.name = ?";
        try {
            return jdbcTemplate.query(findByRoleNameSql, mapper, rolename);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByRoleName", e.getMessage(), rolename);
        }
    }
}
