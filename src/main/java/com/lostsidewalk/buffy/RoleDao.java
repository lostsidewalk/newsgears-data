package com.lostsidewalk.buffy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.sql.Types.VARCHAR;

@Component
public class RoleDao extends AbstractDao<Role> {

    private static final String TABLE_NAME = "roles";

    private final String findByUsernameSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    protected RoleDao() {
        super(TABLE_NAME);
        this.findByUsernameSQL =
                "select * from roles r "
                + " join users_in_roles uir on uir.role = r.name "
                + " join users u on u.name = uir.username "
                + " where u.name = ?";
    }

    private static final String NAME_ATTRIBUTE = "name";

    @Override
    protected String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = newArrayList("name");

    @Override
    protected List<String> getInsertAttributes() {
        return INSERT_ATTRIBUTES;
    }

    @Override
    protected void configureInsertParams(MapSqlParameterSource parameters, Role entity) {
        configureCommonParams(parameters, entity);
    }

    protected void configureUpdateParams(MapSqlParameterSource parameters, Role entity) {
        configureCommonParams(parameters, entity);
    }

    private void configureCommonParams(MapSqlParameterSource parameters, Role entity) {
        parameters.addValue("name", entity.getName(), VARCHAR);
    }

    //
    //
    //

    protected JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    static final RowMapper<Role> ROLE_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");

        String name = rs.getString("name");

        return new Role(id, name);
    };

    @Override
    protected RowMapper<Role> getRowMapper() {
        return ROLE_ROW_MAPPER;
    }

    @SuppressWarnings("unused")
    public List<Role> findByUsername(String username) {
        if (username != null) {
            return getJdbcTemplate().query(this.findByUsernameSQL, getRowMapper(), username);
        }

        return null;
    }
}
