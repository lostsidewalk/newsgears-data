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
import static java.sql.Types.VARCHAR;

/**
 * The RoleDao class is responsible for database operations related to user roles, such as retrieving roles
 * associated with a specific username. It extends the AbstractDao class and provides methods for managing role
 * information in the database.
 */
@Component
public class RoleDao extends AbstractDao<Role> {

    private String findByUsernameSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("${newsgears.data.roles.table}")
    String tableName;

    @Value("${newsgears.data.uir.table}")
    String uirTableName;

    @Override
    protected void setupSQL() {
        this.findByUsernameSQL = String.format(
                "select * from %s r "
                + " join %s uir on uir.role = r.name "
                + " join users u on u.name = uir.username "
                + " where u.name = ? and r.application_id = '%s'",
                getTableName(), uirTableName, applicationId);
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

    @Override
    protected String getTableName() {
        return tableName;
    }

    /**
     * Retrieves a list of roles associated with a given username.
     *
     * @param username The username for which roles need to be retrieved.
     * @return A list of Role objects representing the roles associated with the username.
     */
    @SuppressWarnings("unused")
    public List<Role> findByUsername(String username) {
        if (username != null) {
            return getJdbcTemplate().query(this.findByUsernameSQL, getRowMapper(), username);
        }

        return null;
    }
}
