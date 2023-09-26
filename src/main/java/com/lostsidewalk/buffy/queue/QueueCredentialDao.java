package com.lostsidewalk.buffy.queue;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataConflictException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

import static java.lang.Integer.toUnsignedLong;

/**
 * Data access object for managing queue credentials in the application.
 */
@SuppressWarnings("deprecation")
@Slf4j
@Component
public class QueueCredentialDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String INSERT_QUEUE_CREDENTIALS_SQL =
            "insert into queue_credentials(queue_id, username, basic_username, basic_password, created) values (?,?,?,?,current_timestamp)";

    /**
     * Adds a new queue credential to the database. This method inserts the provided queue credential information
     * into the database, including the associated queue ID, username, basic authentication username, and password.
     *
     * @param queueCredential The queue credential to be added to the database.
     * @return The generated ID of the newly added queue credential.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     * @throws DataConflictException If a data conflict or duplication is encountered during the insertion.
     */
    @SuppressWarnings("unused")
    public Long add(QueueCredential queueCredential) throws DataAccessException, DataUpdateException, DataConflictException {
        Long queueId = queueCredential.getQueueId();
        String username = queueCredential.getUsername();
        String basicUsername = queueCredential.getBasicUsername();
        String basicPassword = queueCredential.getBasicPassword();
        int rowsUpdated;
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            rowsUpdated = jdbcTemplate.update(
                    conn -> {
                        PreparedStatement ps = conn.prepareStatement(INSERT_QUEUE_CREDENTIALS_SQL, new String[] {"id"});
                        ps.setLong(1, queueId);
                        ps.setString(2, username);
                        ps.setString(3, basicUsername);
                        ps.setString(4, basicPassword);

                        return ps;
                    }, keyHolder);
        } catch (DuplicateKeyException e) {
            throw new DataConflictException(getClass().getSimpleName(), "update", e.getMessage(), queueCredential);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), queueId, username, basicUsername);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", queueId, username, basicUsername);
        }
        Integer key = keyHolder.getKeyAs(Integer.class);
        return key == null ? null : toUnsignedLong(key);
    }

    final RowMapper<QueueCredential> QUEUE_CREDENTIAL_ROW_MAPPER = (rs, rowNum) -> {
        Long id = rs.getLong("id");
        Long queueId = rs.getLong("queue_id");
        String username = rs.getString("username");
        String basicUsername = rs.getString("basic_username");
        String basicPassword = rs.getString("basic_password");
        Timestamp created = rs.getTimestamp("created");
        Timestamp lastModified = rs.getTimestamp("last_modified");

        return QueueCredential.from(id, username, queueId, basicUsername, basicPassword, created, lastModified);
    };

    private static final String FIND_BY_ID_SQL = "select * from queue_credentials where username = ? and queue_id = ? and id = ?";

    /**
     * Retrieves a QueueCredential object by the specified username, queueId, and credentialId.
     *
     * @param username The username associated with the credential.
     * @param queueId The identifier of the queue.
     * @param credentialId The identifier of the credential.
     * @return A QueueCredential object if found, or null if not found.
     * @throws DataAccessException If a data access error occurs.
     */
    @SuppressWarnings("unused")
    public QueueCredential findById(String username, Long queueId, Long credentialId) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_ID_SQL, new Object[] { username, queueId }, QUEUE_CREDENTIAL_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findById", e.getMessage(), username, queueId, credentialId);
        }
    }

    private static final String FIND_BY_QUEUE_ID_SQL = "select * from queue_credentials where username = ? and queue_id = ?";

    /**
     * Retrieves a list of QueueCredential objects associated with the specified username and queueId.
     *
     * @param username The username for which to retrieve credentials.
     * @param queueId The identifier of the queue for which to retrieve credentials.
     * @return A list of QueueCredential objects found for the given username and queueId.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public List<QueueCredential> findByQueueId(String username, Long queueId) throws DataAccessException {
        try {
            return jdbcTemplate.query(FIND_BY_QUEUE_ID_SQL, new Object[] { username, queueId }, QUEUE_CREDENTIAL_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByQueueId", e.getMessage(), username, queueId);
        }
    }

    private static final String FIND_BY_REMOTE_USERNAME_SQL = "select * from queue_credentials where queue_id = ? and basic_username = ?";

    /**
     * Retrieves a QueueCredential object by the specified queueId and remoteUsername.
     *
     * @param queueId The identifier of the queue for which to retrieve the credential.
     * @param remoteUsername The remote username associated with the credential.
     * @return A QueueCredential object if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */    @SuppressWarnings("unused")
    public QueueCredential findByRemoteUsername(Long queueId, String remoteUsername) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_REMOTE_USERNAME_SQL, new Object[] { queueId, remoteUsername }, QUEUE_CREDENTIAL_ROW_MAPPER);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findByRemoteUsername", e.getMessage(), queueId, remoteUsername);
        }
    }

    private static final String DELETE_BY_QUEUE_ID = "delete from queue_credentials where username = ? and queue_id = ?";

    /**
     * Deletes all QueueCredential objects associated with the specified username and queueId.
     *
     * @param username The username for which to delete credentials.
     * @param queueId The identifier of the queue for which to delete credentials.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If an error occurs during the data update operation.
     */
    @SuppressWarnings("unused")
    public void deleteByQueueId(String username, Long queueId) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_QUEUE_ID, username, queueId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteByQueueId", e.getMessage(), username, queueId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteByQueueId", username, queueId);
        }
    }

    private static final String DELETE_BY_ID_SQL = "delete from queue_credentials where username = ? and queue_id = ? and id = ?";

    /**
     * Deletes a QueueCredential entry by the specified username, queueId, and credentialId.
     *
     * @param username The username associated with the credential.
     * @param queueId The identifier of the queue to which the credential belongs.
     * @param credentialId The identifier of the credential to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the deletion operation fails or no rows were affected.
     */
    @SuppressWarnings("unused")
    public void deleteById(String username, Long queueId, Long credentialId) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_BY_ID_SQL, username, queueId, credentialId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteById", e.getMessage(), username, queueId, credentialId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteById", username, queueId, credentialId);
        }
    }

    private static final String UPDATE_CREDENTIAL_BY_ID_SQL = "update queue_credentials set basic_password = ?, last_modified = current_timestamp where username = ? and queue_id = ? and id = ?";

    /**
     * Updates the password of a QueueCredential entry identified by the specified username, queueId, and credentialId.
     *
     * @param username The username associated with the credential.
     * @param queueId The identifier of the queue to which the credential belongs.
     * @param credentialId The identifier of the credential to be updated.
     * @param password The new password to set for the credential.
     * @throws DataAccessException If an error occurs while accessing the data.
     * @throws DataUpdateException If the update operation fails or no rows were affected.
     */
    @SuppressWarnings("unused")
    public void updatePassword(String username, Long queueId, Long credentialId, String password) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_CREDENTIAL_BY_ID_SQL, password, username, queueId, credentialId);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updatePassword", e.getMessage(), username, queueId, credentialId);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePassword", username, queueId, credentialId);
        }
    }
}
