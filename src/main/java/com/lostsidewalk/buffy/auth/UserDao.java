package com.lostsidewalk.buffy.auth;

import com.google.common.collect.ImmutableMap;
import com.lostsidewalk.buffy.AbstractDao;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

/**
 * Data access object for managing users in the application.
 */
@Slf4j
@Component
public class UserDao extends AbstractDao<User> {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Value("${newsgears.data.users.table}")
    String tableName;

    @Value("${newsgears.data.apikeys.table}")
    String apiKeysTableName;

    private static final String FIND_BY_EMAIL_ADDRESS_SQL = "select * from %s u where email_address = ? and application_id = '%s'";
    private static final String FIND_BY_CUSTOMER_ID_SQL = "select * from %s u where customer_id = ? and application_id = '%s'";
    private static final String FIND_BY_AUTH_PROVIDER_ID_SQL = "select * from %s u where auth_provider = ? and auth_provider_id = ? and application_id = '%s'";
    private static final String FIND_BY_API_KEY_SQL = "select * from %s u join %s a on u.id = a.user_id where a.api_key = ?";
    private static final String COUNT_BY_USERNAME_OR_EMAIL_ADDRESS_SQL = "select count(*) from %s where (name = ? or email_address = ?) and application_id = '%s'";
    private static final String SET_VERIFIED_SQL = "update %s set is_verified = ? where name = ? and application_id = '%s'";

    private String findByEmailAddressSQL;
    private String findByCustomerIdSQL;
    private String findByAuthProviderIdSQL;
    private String findByApiKeySQL;
    private String countByUsernameOrEmailAddressSQL;
    private String setVerifiedSQL;

    private static final String UPDATE_AUTH_CLAIM_SQL = "update %s set auth_claim = ? where id = ?";
    private static final String UPDATE_PW_RESET_CLAIM_SQL = "update %s set pw_reset_claim = ? where id = ?";
    private static final String UPDATE_VERIFICATION_CLAIM_SQL = "update %s set verification_claim = ? where id = ?";
    private static final String UPDATE_PW_RESET_AUTH_CLAIM_SQL = "update %s set pw_reset_auth_claim = ? where id = ?";
    private static final String UPDATE_PASSWORD_SQL = "update %s set password = ? where id = ?";
    private static final String UPDATE_EMAIL_ADDRESS_SQL = "update %s set email_address = ? where id = ?";
    private static final String UPDATE_CUSTOMER_ID_SQL = "update %s set customer_id = ? where id = ?";
    private static final String UPDATE_SUBSCRIPTION_STATUS_SQL = "update %s set subscription_status = ? where id = ?";
    private static final String UPDATE_SUBSCRIPTION_EXP_DATE_SQL = "update %s set subscription_exp_date = ? where id = ?";

    private String updateAuthClaimSql;
    private String updatePwResetClaimSql;
    private String updateVerificationClaimSql;
    private String updatePwResetAuthClaimSql;
    private String updatePasswordSql;
    private String updateEmailAddressSql;
    private String updateCustomerIdSql;
    private String updateSubscriptionStatusSql;
    private String updateSubscriptionExpDateSql;

    @Override
    protected void setupSQL() {
        this.findByEmailAddressSQL = String.format(FIND_BY_EMAIL_ADDRESS_SQL, tableName, applicationId);
        this.findByCustomerIdSQL = String.format(FIND_BY_CUSTOMER_ID_SQL, tableName, applicationId);
        this.findByAuthProviderIdSQL = String.format(FIND_BY_AUTH_PROVIDER_ID_SQL, tableName, applicationId);
        this.findByApiKeySQL = String.format(FIND_BY_API_KEY_SQL, tableName, apiKeysTableName);
        this.countByUsernameOrEmailAddressSQL = String.format(COUNT_BY_USERNAME_OR_EMAIL_ADDRESS_SQL, tableName, applicationId);
        this.setVerifiedSQL = String.format(SET_VERIFIED_SQL, tableName, applicationId);

        this.updateAuthClaimSql = String.format(UPDATE_AUTH_CLAIM_SQL, tableName);
        this.updatePwResetClaimSql = String.format(UPDATE_PW_RESET_CLAIM_SQL, tableName);
        this.updateVerificationClaimSql = String.format(UPDATE_VERIFICATION_CLAIM_SQL, tableName);
        this.updatePwResetAuthClaimSql = String.format(UPDATE_PW_RESET_AUTH_CLAIM_SQL, tableName);
        this.updatePasswordSql = String.format(UPDATE_PASSWORD_SQL, tableName);
        this.updateEmailAddressSql = String.format(UPDATE_EMAIL_ADDRESS_SQL, tableName);
        this.updateCustomerIdSql = String.format(UPDATE_CUSTOMER_ID_SQL, tableName);
        this.updateSubscriptionStatusSql = String.format(UPDATE_SUBSCRIPTION_STATUS_SQL, tableName);
        this.updateSubscriptionExpDateSql = String.format(UPDATE_SUBSCRIPTION_EXP_DATE_SQL, tableName);
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

    @Override
    protected String getTableName() {
        return tableName;
    }

    /**
     * Retrieves a user by their email address.
     *
     * @param emailAddress The email address of the user to retrieve.
     * @return The User object corresponding to the provided email address, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public User findByEmailAddress(String emailAddress) throws DataAccessException {
        if (isNotBlank(emailAddress)) {
            try {
                List<User> results = getJdbcTemplate().query(findByEmailAddressSQL, getRowMapper(), emailAddress);
                return isEmpty(results) ? null : results.get(0); // email_address should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findByEmailAddress", e.getMessage(), emailAddress);
            }
        }

        return null;
    }

    /**
     * Retrieves a user by their customer ID.
     *
     * @param customerId The customer ID of the user to retrieve.
     * @return The User object corresponding to the provided customer ID, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public User findByCustomerId(String customerId) throws DataAccessException {
        if (isNotBlank(customerId)) {
            try {
                List<User> results = getJdbcTemplate().query(findByCustomerIdSQL, getRowMapper(), customerId);
                return isEmpty(results) ? null : results.get(0); // customer_id should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findByCustomerId", e.getMessage(), customerId);
            }
        }

        return null;
    }

    /**
     * Retrieves a user by their authentication provider and provider ID.
     *
     * @param authProvider The authentication provider (e.g., OAuth, Google, etc.).
     * @param authProviderId The unique ID associated with the user from the authentication provider.
     * @return The User object corresponding to the provided authentication provider and ID, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public User findByAuthProviderId(AuthProvider authProvider, String authProviderId) throws DataAccessException {
        if (authProvider != null && isNotBlank(authProviderId)) {
            try {
                List<User> results = getJdbcTemplate().query(findByAuthProviderIdSQL, getRowMapper(), authProvider.name(), authProviderId);
                return isEmpty(results) ? null : results.get(0); // auth_provider, auth_provider_id should be unique
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findByAuthProviderId", e.getMessage(), authProvider, authProviderId);
            }
        }

        return null;
    }

    /**
     * Retrieves a user by their API key.
     *
     * @param apiKey The API key of the user to retrieve.
     * @return The User object corresponding to the provided API key, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public User findByApiKey(String apiKey) throws DataAccessException {
        if (isNotBlank(apiKey)) {
            try {
                List<User> results = getJdbcTemplate().query(findByApiKeySQL, getRowMapper(), apiKey);
                return isEmpty(results) ? null : results.get(0);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "findByApiKey", e.getMessage(), apiKey);
            }
        }

        return null;
    }

    /**
     * Checks if a user with the specified username and email address exists in the database.
     *
     * @param username The username of the user to check.
     * @param emailAddress The email address of the user to check.
     * @return True if a user with the provided username and email address exists; otherwise, false.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public boolean checkExists(String username, String emailAddress) throws DataAccessException {
        if (isNotBlank(username) && isNotBlank(emailAddress)) {
            try {
                Integer ct = getJdbcTemplate().queryForObject(countByUsernameOrEmailAddressSQL, Integer.class, username, emailAddress);
                return ct != null && ct > 0;
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "checkExists", e.getMessage(), username, emailAddress);
            }
        }

        return false;
    }

    /**
     * Sets the verification status of a user with the specified username.
     *
     * @param username The username of the user to set the verification status for.
     * @param isVerified The verification status to set (true for verified, false for unverified).
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void setVerified(String username, boolean isVerified) throws DataAccessException, DataUpdateException {
        if (isNotBlank(username)) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(setVerifiedSQL, isVerified, username);
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "setVerified", e.getMessage(), username, isVerified);
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "setVerified", username, isVerified);
            }
        }
    }

    /**
     * Updates the authentication claim of a user.
     *
     * @param user The user for which to update the authentication claim.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateAuthClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateAuthClaimSql, user.getAuthClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updateAuthClaim", e.getMessage(), user.getAuthClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateAuthClaim", user.getAuthClaim(), user.getId());
            }
        }
    }

    /**
     * Updates the password reset claim of a user.
     *
     * @param user The user for which to update the password reset claim.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updatePwResetClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updatePwResetClaimSql, user.getPwResetClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updatePwResetClaim", e.getMessage(), user.getPwResetClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePwResetClaim", user.getPwResetClaim(), user.getId());
            }
        }
    }

    /**
     * Updates the verification claim of a user.
     *
     * @param user The user for which to update the verification claim.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateVerificationClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateVerificationClaimSql, user.getVerificationClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updateVerificationClaim", e.getMessage(), user.getVerificationClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateVerificationClaim", user.getVerificationClaim(), user.getId());
            }
        }
    }

    /**
     * Updates the password reset authentication claim of a user.
     *
     * @param user The user for which to update the password reset authentication claim.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updatePwResetAuthClaim(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updatePwResetAuthClaimSql, user.getPwResetAuthClaim(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updatePwResetAuthClaim", e.getMessage(), user.getPwResetAuthClaim(), user.getId());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePwResetAuthClaim", user.getPwResetClaim(), user.getId());
            }
        }
    }

    /**
     * Updates the password of a user.
     *
     * @param user The user for which to update the password.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updatePassword(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updatePasswordSql, user.getPassword(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updatePassword", e.getMessage(), user.getId()); // password not shown
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updatePassword", user.getId()); // password not shown
            }
        }
    }

    /**
     * Updates the email address of a user.
     *
     * @param user The user for which to update the email address.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateEmailAddress(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateEmailAddressSql, user.getEmailAddress());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updateEmailAddress", e.getMessage(), user.getId(), user.getEmailAddress());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateEmailAddress", user.getId(), user.getEmailAddress());
            }
        }
    }

    /**
     * Updates the customer ID of a user.
     *
     * @param user The user for which to update the customer ID.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateCustomerId(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateCustomerIdSql, user.getCustomerId(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updateCustomerId", e.getMessage(), user.getId(), user.getEmailAddress());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateCustomerId", user.getId(), user.getEmailAddress());
            }
        }
    }

    /**
     * Updates the subscription status of a user.
     *
     * @param user The user for which to update the subscription status.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateSubscriptionStatus(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateSubscriptionStatusSql, user.getSubscriptionStatus(), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
                throw new DataAccessException(getClass().getSimpleName(), "updateSubscriptionStatus", e.getMessage(), user.getId(), user.getSubscriptionStatus());
            }
            if (!(rowsUpdated > 0)) {
                throw new DataUpdateException(getClass().getSimpleName(), "updateSubscriptionStatus", user.getId(), user.getSubscriptionStatus());
            }
        }
    }

    /**
     * Updates the subscription expiration date of a user.
     *
     * @param user The user for which to update the subscription expiration date.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails.
     */
    @SuppressWarnings("unused")
    public void updateSubscriptionExpDate(User user) throws DataAccessException, DataUpdateException {
        if (user != null) {
            int rowsUpdated;
            try {
                rowsUpdated = getJdbcTemplate().update(updateSubscriptionExpDateSql, toTimestamp(user.getSubscriptionExpDate()), user.getId());
            } catch (Exception e) {
                log.error("Something horrible happened due to: {}", e.getMessage());
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
