package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;

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

    @Override
    public final String toString() {
        return "RenderedFeedDao{" +
                "redisTemplate=" + redisTemplate +
                '}';
    }
}
