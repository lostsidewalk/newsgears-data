package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.io.Serializable;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class provides data access methods for storing and retrieving rendered feed data
 * using Redis as the storage backend.
 *
 * @see RenderedRSSFeed
 * @see RenderedATOMFeed
 */
@SuppressWarnings("OverlyBroadCatchBlock")
@Slf4j
@Component
@Profile("redis")
public class RenderedFeedDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    /**
     * Default constructor; initializes the object.
     */
    RenderedFeedDao() {
    }

    //
    // RSS feed methods
    //

    /**
     * Retrieves a rendered RSS feed channel by its transport identifier.
     *
     * @param transportIdent The transport identifier of the RSS feed channel.
     * @return The rendered RSS feed channel if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final RenderedRSSFeed findRSSChannelByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedRSSFeed> hashOps = redisTemplate.opsForHash();
            return hashOps.get("RENDERED_RSS_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findRSSChannelByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    /**
     * Stores a rendered RSS feed at the specified transport identifier.
     *
     * @param transportIdent  The transport identifier where the RSS feed will be stored.
     * @param renderedRSSFeed The rendered RSS feed to be stored.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final void putRSSFeedAtTransportIdent(String transportIdent, RenderedRSSFeed renderedRSSFeed) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedRSSFeed> hashOps = redisTemplate.opsForHash();
            hashOps.put("RENDERED_RSS_FEEDS", transportIdent, renderedRSSFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "putRSSFeedAtTransportIdent", e.getMessage(), transportIdent, renderedRSSFeed);
        }
    }
    //
    // ATOM feed methods
    //

    /**
     * Retrieves a rendered ATOM feed by its transport identifier.
     *
     * @param transportIdent The transport identifier of the ATOM feed.
     * @return The rendered ATOM feed if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final RenderedATOMFeed findATOMFeedByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedATOMFeed> hashOps = redisTemplate.opsForHash();
            return hashOps.get("RENDERED_ATOM_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findATOMFeedByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    /**
     * Stores a rendered ATOM feed at the specified transport identifier.
     *
     * @param transportIdent   The transport identifier where the ATOM feed will be stored.
     * @param renderedATOMFeed The rendered ATOM feed to be stored.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final void putATOMFeedAtTransportIdent(String transportIdent, RenderedATOMFeed renderedATOMFeed) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedATOMFeed> hashOps = redisTemplate.opsForHash();
            hashOps.put("RENDERED_ATOM_FEEDS", transportIdent, renderedATOMFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "putATOMFeedAtTransportIdent", e.getMessage(), transportIdent, renderedATOMFeed);
        }
    }
    // 
    // JSON feed methods 
    //

    /**
     * Retrieves a rendered JSON feed by its transport identifier.
     *
     * @param <T>            The type of the rendered JSON feed.
     * @param transportIdent The transport identifier of the JSON feed.
     * @return The rendered JSON feed if found, or null if not found.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final <T extends Serializable> T findJSONFeedByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, T> hashOps = redisTemplate.opsForHash();
            return hashOps.get("RENDERED_JSON_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findJSONFeedByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    /**
     * Stores a rendered JSON feed at the specified transport identifier.
     *
     * @param <T>              The type of the rendered JSON feed.
     * @param transportIdent   The transport identifier where the JSON feed will be stored.
     * @param renderedJSONFeed The rendered JSON feed to be stored.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final <T extends Serializable> void putJSONFeedAtTransportIdent(String transportIdent, T renderedJSONFeed) throws DataAccessException {
        try {
            HashOperations<String, String, T> hashOps = redisTemplate.opsForHash();
            hashOps.put("RENDERED_JSON_FEEDS", transportIdent, renderedJSONFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "putJSONFeedAtTransportIdent", e.getMessage(), transportIdent, renderedJSONFeed);
        }
    }
    //
    //
    //

    /**
     * Deletes a rendered feed at the specified transport identifier.
     *
     * @param transportIdent The transport identifier of the feed to be deleted.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final void deleteFeedAtTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, Object> hashOps = redisTemplate.opsForHash();
            hashOps.delete("RENDERED_RSS_FEEDS", transportIdent);
            hashOps.delete("RENDERED_ATOM_FEEDS", transportIdent);
            hashOps.delete("RENDERED_JSON_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "deleteFeedAtTransportIdent", e.getMessage(), transportIdent);
        }
    }

    //
    //
    //


    //
    //
    //

    private static final int MAX_RETRIES = 3;
    private static final int LOCK_TIMEOUT_S = 30; // Lock expiration time in S
    private static final int RETRY_TIMEOUT_MS = 100; // Retry interval in MS

    /**
     * Acquires a lock on a resource with retry and error handling.
     *
     * @param lockKey   Unique identifier of the locked resource.
     * @param lockValue Unique identifier of the entity holding the lock.
     * @return True if the lock is successfully acquired, false if it fails after multiple retries.
     */
    @SuppressWarnings("unused")
    public final boolean acquireLockWithRetry(String lockKey, String lockValue) {
        log.debug("Attempting to acquire lock with retry, lockKey={}, lockValue={}", lockKey, lockValue);
        for (int retry = 0; MAX_RETRIES > retry; retry++) {
            boolean lockAcquired = acquireLock(lockKey, lockValue);
            if (lockAcquired) {
                log.debug("Redis lock acquired, lockKey={}, lockValue={}", lockKey, lockValue);
                return true;
            } else {
                log.debug("Failed to acquire redis lock, retry={}, , lockKey={}, lockValue={}", retry, lockKey, lockValue);
            }
            try {
                Thread.sleep(RETRY_TIMEOUT_MS);
            } catch (InterruptedException e) {
                log.info("Retry timeout exceeded, lockKey={}, lockValue={}", lockKey, lockValue);
                Thread.currentThread().interrupt();
            }
        }

        log.debug("Abandoning attempt to acquire lock, lockKey={}, lockValue={}", lockKey, lockValue);
        return false;
    }

    private boolean acquireLock(String lockKey, String lockValue) {
        Boolean result = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_S, SECONDS);
        return null != result && result;
    }

    private static final RedisScript<Long> releaseLockScript = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    /**
     * Releases a lock on a resource held by the given entity.
     *
     * @param lockKey   Unique identifier of the locked resource.
     * @param lockValue Unique identifier of the entity holding the lock.
     * @return True if the lock is successfully released, false otherwise.
     */
    @SuppressWarnings("unused")
    public final boolean releaseLock(String lockKey, String lockValue) {
        log.debug("Attempting to release lock, lockValue={}", lockValue);
        Long result = redisTemplate.execute(releaseLockScript, singletonList(lockKey), lockValue);
        boolean release = null != result && 1 == result;

        log.debug("Lock release result={}, lockKey={}, lockValue={}", result, lockKey, lockValue);
        return release;
    }

    @Override
    public final String toString() {
        return "RenderedFeedDao{" +
                "redisTemplate=" + redisTemplate +
                '}';
    }
}
