package com.lostsidewalk.buffy.auth;

import com.google.common.collect.ImmutableMap;
import com.lostsidewalk.buffy.AbstractDao;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.sql.Types.*;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@Component
public class UserDao extends AbstractDao<User> {

    private static final String TABLE_NAME = "users";

    private static final String findByEmailAddressSQL = "select * from users u where email_address = ?";

    private static final String findByCustomerIdSQL = "select * from users u where customer_id = ?";

    private static final String findByAuthProviderIdSQL = "select * from users u where auth_provider = ? and auth_provider_id = ?";

    private static final String countByUsernameOrEmailAddressSQL = "select count(*) from users where (name = ? or email_address = ?)";

    private static final String setVerifiedSQL = "update users set is_verified = ? where name = ?";

    @Autowired
    JdbcTemplate jdbcTemplate;

    protected UserDao() {
        super(TABLE_NAME);
    }

    private static final String NAME_ATTRIBUTE = "name";

    @Override
    protected String getNameAttribute() {
        return NAME_ATTRIBUTE;
    }

    //
    //
    //

    private static final List<String> INSERT_ATTRIBUTES = newArrayList(
            "name",
            "password",
            "email_address",
            "auth_claim",
            "pw_reset_claim",
            "pw_reset_auth_claim",
            "verification_claim",
            "is_verified",
            "subscription_status",
            "subscription_exp_date",
            "customer_id",
            "auth_provider",
            "auth_provider_id",
            "auth_provider_profile_img_url",
            "auth_provider_username"
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
        parameters.addValue("pw_reset_claim", entity.getPwResetClaim(), VARCHAR);
        parameters.addValue("pw_reset_auth_claim", entity.getPwResetAuthClaim(), VARCHAR);
        parameters.addValue("verification_claim", entity.getVerificationClaim(), VARCHAR);
        parameters.addValue("is_verified", entity.isVerified(), BOOLEAN);
        parameters.addValue("subscription_status", entity.getSubscriptionStatus(), VARCHAR);
        parameters.addValue("subscription_exp_date", toTimestamp(entity.getSubscriptionExpDate()), TIMESTAMP);
        parameters.addValue("customer_id", entity.getCustomerId(), VARCHAR);
        parameters.addValue("auth_provider", entity.getAuthProvider(), VARCHAR);
        parameters.addValue("auth_provider_id", entity.getAuthProviderId(), VARCHAR);
        parameters.addValue("auth_provider_profile_img_url", entity.getAuthProviderProfileImgUrl(), VARCHAR);
        parameters.addValue("auth_provider_username", entity.getAuthProviderUsername(), VARCHAR);
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

        String pwResetClaim = rs.getString("pw_reset_claim");
        u.setPwResetClaim(pwResetClaim);

        String pwResetAuthClaim = rs.getString("pw_reset_auth_claim");
        u.setPwResetAuthClaim(pwResetAuthClaim);

        String verificationClaim = rs.getString("verification_claim");
        u.setVerificationClaim(verificationClaim);

        boolean isVerified = rs.getBoolean("is_verified");
        u.setVerified(isVerified);

        String subscriptionStatus = rs.getString("subscription_status");
        u.setSubscriptionStatus(subscriptionStatus);

        Timestamp subscriptionExpDate = rs.getTimestamp("subscription_exp_date");
        u.setSubscriptionExpDate(subscriptionExpDate);

        String customerId = rs.getString("customer_id");
        u.setCustomerId(customerId);

        String authProvider = rs.getString("auth_provider");
        u.setAuthProvider(AuthProvider.valueOf(authProvider));

        String authProviderId = rs.getString("auth_provider_id");
        u.setAuthProviderId(authProviderId);

        String authProviderProfileImgUrl = rs.getString("auth_provider_profile_img_url");
        u.setAuthProviderProfileImgUrl(authProviderProfileImgUrl);

        String authProviderUsername = rs.getString("auth_provider_username");
        u.setAuthProviderUsername(authProviderUsername);

        return u;
    };

    @Override
    protected RowMapper<User> getRowMapper() {
        return USER_ROW_MAPPER;
    }

    @SuppressWarnings("unused")
    public User findByEmailAddress(String emailAddress) throws DataAccessException {
        if (isNotBlank(emailAddress)) {
            try {
                List<User> results = getJdbcTemplate().query(findByEmailAddressSQL, getRowMapper(), emailAddress);
                return isEmpty(results) ? null : results.get(0); // email_address should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "findByEmailAddress", e.getMessage(), emailAddress);
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    public User findByCustomerId(String customerId) throws DataAccessException {
        if (isNotBlank(customerId)) {
            try {
                List<User> results = getJdbcTemplate().query(findByCustomerIdSQL, getRowMapper(), customerId);
                return isEmpty(results) ? null : results.get(0); // customer_id should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "findByCustomerId", e.getMessage(), customerId);
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    public User findByAuthProviderId(AuthProvider authProvider, String authProviderId) throws DataAccessException {
        if (authProvider != null && isNotBlank(authProviderId)) {
            try {
                List<User> results = getJdbcTemplate().query(findByAuthProviderIdSQL, getRowMapper(), authProvider.name(), authProviderId);
                return isEmpty(results) ? null : results.get(0); // auth_provider, auth_provider_id should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "findByAuthProviderId", e.getMessage(), authProvider, authProviderId);
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    public boolean checkExists(String username, String emailAddress) throws DataAccessException {
        if (isNotBlank(username) && isNotBlank(emailAddress)) {
            try {
                Integer ct = getJdbcTemplate().queryForObject(countByUsernameOrEmailAddressSQL, Integer.class, username, emailAddress);
                return ct != null && ct > 0;
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), username, emailAddress);
            }
        }

        return false;
    }

    @SuppressWarnings("unused")
    public void setVerified(String username, boolean isVerified) throws DataAccessException, DataUpdateException {
        if (isNotBlank(username)) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(setVerifiedSQL, isVerified, username);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "setVerified", e.getMessage(), username, isVerified);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "setVerified", username, isVerified);
            }
        }
    }

    private static final String UPDATE_AUTH_CLAIM_SQL = "update users set auth_claim = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateAuthClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_AUTH_CLAIM_SQL, user.getAuthClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateAuthClaim", e.getMessage(), user.getAuthClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateAuthClaim", user.getAuthClaim(), user.getId());
            }
        }
    }

    private static final String UPDATE_PW_RESET_CLAIM_SQL = "update users set pw_reset_claim = ? where id = ?";

    @SuppressWarnings("unused")
    public void updatePwResetClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_PW_RESET_CLAIM_SQL, user.getPwResetClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updatePwResetClaim", e.getMessage(), user.getPwResetClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePwResetClaim", user.getPwResetClaim(), user.getId());
            }
        }
    }

    private static final String UPDATE_VERIFICATION_CLAIM_SQL = "update users set verification_claim = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateVerificationClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_VERIFICATION_CLAIM_SQL, user.getVerificationClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateVerificationClaim", e.getMessage(), user.getVerificationClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateVerificationClaim", user.getVerificationClaim(), user.getId());
            }
        }
    }

    private static final String UPDATE_PW_RESET_AUTH_CLAIM_SQL = "update users set pw_reset_auth_claim = ? where id = ?";

    @SuppressWarnings("unused")
    public void updatePwResetAuthClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_PW_RESET_AUTH_CLAIM_SQL, user.getPwResetAuthClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updatePwResetAuthClaim", e.getMessage(), user.getPwResetAuthClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePwResetAuthClaim", user.getPwResetClaim(), user.getId());
            }
        }
    }

    private static final String UPDATE_PASSWORD_SQL = "update users set password = ? where id = ?";

    @SuppressWarnings("unused")
    public void updatePassword(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_PASSWORD_SQL, user.getPassword(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updatePassword", e.getMessage(), user.getId()); // password not shown
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePassword", user.getId()); // password not shown
            }
        }
    }

    private static final String UPDATE_EMAIL_ADDRESS_SQL = "update users set email_address = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateEmailAddress(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_EMAIL_ADDRESS_SQL, user.getEmailAddress());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateEmailAddress", e.getMessage(), user.getId(), user.getEmailAddress());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateEmailAddress", user.getId(), user.getEmailAddress());
            }
        }
    }

    private static final String UPDATE_CUSTOMER_ID_SQL = "update users set customer_id = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateCustomerId(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_CUSTOMER_ID_SQL, user.getCustomerId(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateCustomerId", e.getMessage(), user.getId(), user.getEmailAddress());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateCustomerId", user.getId(), user.getEmailAddress());
            }
        }
    }

    private static final String UPDATE_SUBSCRIPTION_STATUS_SQL = "update users set subscription_status = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateSubscriptionStatus(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_SUBSCRIPTION_STATUS_SQL, user.getSubscriptionStatus(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateSubscriptionStatus", e.getMessage(), user.getId(), user.getSubscriptionStatus());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateSubscriptionStatus", user.getId(), user.getSubscriptionStatus());
            }
        }
    }

    private static final String UPDATE_SUBSCRIPTION_EXP_DATE_SQL = "update users set subscription_exp_date = ? where id = ?";

    @SuppressWarnings("unused")
    public void updateSubscriptionExpDate(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(UPDATE_SUBSCRIPTION_EXP_DATE_SQL, toTimestamp(user.getSubscriptionExpDate()), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage(), e);
                throw new DataAccessException(getClass().getSimpleName(), "updateSubscriptionExpDate", e.getMessage(), user.getId(), user.getSubscriptionExpDate());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateSubscriptionExpDate", user.getId(), user.getSubscriptionExpDate());
            }
        }
    }

    //
    //
    //

    private static final ImmutableMap<String, Integer> NULLABLE_SQL_TYPES = ImmutableMap.of();

    @Override
    protected int getSqlType(String parameterName) {
        return NULLABLE_SQL_TYPES.getOrDefault(parameterName, super.getSqlType(parameterName));
    }

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private static Timestamp toTimestamp(Date d) {
        Instant i = d != null ? OffsetDateTime.from(d.toInstant().atZone(ZONE_ID)).toInstant() : null;
        return i != null ? Timestamp.from(i) : null;
    }
}
