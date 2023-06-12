package com.lostsidewalk.buffy.queue;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FeedCredentialsDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String INSERT_FEED_CREDENTIALS_SQL =
            "insert into feed_credentials(transport_ident, username, password) values (?,?,?)";

    @SuppressWarnings("unused")
    public void add(String transportIdent, String username, String password) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(INSERT_FEED_CREDENTIALS_SQL, transportIdent, username, password);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "add", e.getMessage(), transportIdent, username);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "add", transportIdent, username);
        }
    }

    private static final String FIND_BY_TRANSPORT_IDENT_SQL = "select password from feed_credentials where transport_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public String findByTransportIdent(String username, String transportIdent) throws DataAccessException {
        try {
            return jdbcTemplate.queryForObject(FIND_BY_TRANSPORT_IDENT_SQL, new Object[] { transportIdent, username }, String.class);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findByTransportIdent", e.getMessage(), username, transportIdent);
        }
    }

    private static final String DELETE_USER_BY_TRANSPORT_IDENT = "delete from feed_credentials where transport_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public void deleteUserByTransportIdent(String transportIdent, String username) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(DELETE_USER_BY_TRANSPORT_IDENT, transportIdent, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteUserByTransportIdent", e.getMessage(), transportIdent, username);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "deleteUserByTransportIdent", transportIdent, username);
        }
    }

    private static final String UPDATE_BY_TRANSPORT_IDENT_SQL = "update feed_credentials set password = ? where transport_ident = ? and username = ?";

    @SuppressWarnings("unused")
    public void updatePassword(String transportIdent, String username, String password) throws DataAccessException, DataUpdateException {
        int rowsUpdated;
        try {
            rowsUpdated = jdbcTemplate.update(UPDATE_BY_TRANSPORT_IDENT_SQL, password, transportIdent, username);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updatePassword", e.getMessage(), transportIdent, username);
        }
        if (!(rowsUpdated > 0)) {
            throw new DataUpdateException(getClass().getSimpleName(), "updatePassword", transportIdent, username);
        }
    }
}
