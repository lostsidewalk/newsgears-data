package com.lostsidewalk.buffy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FeatureDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final RowMapper<String> mapper = (rs, rowNum) -> rs.getString("feature_cd");

    public List<String> findByRolename(String rolename) {
        String findByRoleNameSql = "select feature_cd from features_in_roles fir "
                + " join roles r on r.name = fir.role "
                + " where r.name = ?";
        return jdbcTemplate.query(findByRoleNameSql, mapper, rolename);
    }
}
