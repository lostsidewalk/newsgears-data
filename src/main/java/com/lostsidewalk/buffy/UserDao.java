package com.lostsidewalk.buffy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARCHAR;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;

@Component
public class UserDao extends AbstractDao<User> {

    private static final String TABLE_NAME = "users";

    private final String findByEmailAddressSQL;

    @Autowired
    JdbcTemplate jdbcTemplate;

    protected UserDao() {
        super(TABLE_NAME);
        this.findByEmailAddressSQL = "select * from users u where email_address = ?";
    }

    private static final String NAME_ATTRIBUTE = "name";

    @Override
    protected String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = Lists.newArrayList(
            "name",
            "password",
            "email_address",
            "auth_claim"
    );

    @Override
    protected List<String> getInsertAttributes() {
        return INSERT_ATTRIBUTES;
    }

    @Override
    protected void configureInsertParams(MapSqlParameterSource parameters, User entity) {
        configureCommonParams(parameters, entity);
    }

    @Override
    protected void configureUpdateParams(MapSqlParameterSource parameters, User entity) {
        configureCommonParams(parameters, entity);
        parameters.addValue("id", entity.getId(), NUMERIC);
    }

    private void configureCommonParams(MapSqlParameterSource parameters, User entity) {
        parameters.addValue("name", entity.getUsername(), VARCHAR);
        parameters.addValue("password", entity.getPassword(), VARCHAR);
        parameters.addValue("email_address", entity.getEmailAddress(), VARCHAR);
        parameters.addValue("auth_claim", entity.getAuthClaim(), VARCHAR);
    }

    @Override
    protected JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }

    static final RowMapper<User> USER_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");

        String username = rs.getString("name");

        String password = rs.getString("password");

        String emailAddress = rs.getString("email_address");

        User u = new User(id, username, password, emailAddress);
        String authClaim = rs.getString("auth_claim");
        u.setAuthClaim(authClaim);

        return u;
    };

    @Override
    protected RowMapper<User> getRowMapper() {
        return USER_ROW_MAPPER;
    }

    public User findByEmailAddress(String emailAddress) {
        if (emailAddress != null) {
            List<User> results = getJdbcTemplate().query(this.findByEmailAddressSQL, getRowMapper(), emailAddress);
            return isEmpty(results) ? null : results.get(0); // email_address should be unique
        }

        return null;
    }

    private static final ImmutableMap<String, Integer> NULLABLE_SQL_TYPES = ImmutableMap.of(
            "verification_token", VARCHAR
    );

    @Override
    protected int getSqlType(String parameterName) {
        return NULLABLE_SQL_TYPES.getOrDefault(parameterName, super.getSqlType(parameterName));
    }
}
