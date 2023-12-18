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

import static java.sql.Types.VARCHAR;

/**
 * The RoleDao class is responsible for database operations related to user roles, such as retrieving roles
 * associated with a specific username. It extends the AbstractDao class and provides methods for managing role
 * information in the database.
 */
@Slf4j
@Component
public class RoleDao extends AbstractDao<Role> {

    private String findByUsernameSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("${newsgears.data.roles.table}")
    String tableName;

    @Value("${newsgears.data.uir.table}")
    String uirTableName;

    /**
     * Default constructor; initializes the object.
     */
    RoleDao() {
    }

    @Override
    protected final void setupSQL() {
        findByUsernameSQL = String.format(
                "select * from %s r "
                + " join %s uir on uir.role = r.name "
                + " join users u on u.name = uir.username "
                + " where u.name = ? and r.application_id = '%s'",
                tableName, uirTableName, getApplicationId());
    }

    private static final String NAME_ATTRIBUTE = "name";

    @SuppressWarnings("MethodReturnAlwaysConstant")
    @Override
    protected final String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = ImmutableList.of("name");

    @Override
    protected final List<String> getInsertAttributes() {
        return INSERT_ATTRIBUTES;
    }

    @Override
    protected final void configureInsertParams(MapSqlParameterSource parameters, Role entity) {
        configureCommonParams(parameters, entity);
    }

    @Override
    protected final void setId(Role entity, long id) {
        entity.setId(id);
    }

    protected final void configureUpdateParams(MapSqlParameterSource parameters, Role entity) {
        configureCommonParams(parameters, entity);
    }

    private static void configureCommonParams(MapSqlParameterSource parameters, Role entity) {
        parameters.addValue("name", entity.getName(), VARCHAR);
    }

    //
    //
    //

    protected final JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    private static final RowMapper<Role> ROLE_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");

        String name = rs.getString("name");

        return new Role(id, name);
    };

    @Override
    protected final RowMapper<Role> getRowMapper() {
        return ROLE_ROW_MAPPER;
    }

    @Override
    protected final String getTableName() {
        return tableName;
    }

    @Override
    protected final String getDescription() {
        return "findByUsernameSQL='" + findByUsernameSQL + '\'' +
                ", jdbcTemplate=" + jdbcTemplate +
                ", tableName='" + tableName + '\'' +
                ", uirTableName='" + uirTableName + '\'';
    }

    /**
     * Retrieves a list of roles associated with a given username.
     *
     * @param username The username for which roles need to be retrieved.
     * @return A list of Role objects representing the roles associated with the username.
     */
    @SuppressWarnings("unused")
    public final List<Role> findByUsername(String username) {
        if (null != username) {
            return jdbcTemplate.query(findByUsernameSQL, ROLE_ROW_MAPPER, username);
        }

        return null;
    }
}
