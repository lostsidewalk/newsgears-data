package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * This class provides data access methods for storing and retrieving rendered feed catalog information
 * using Redis as the storage backend.
 *
 * @see RenderedFeedDiscoveryInfo
 */
@SuppressWarnings("OverlyBroadCatchBlock")
@Slf4j
@Component
@Profile("redis")
public class RenderedCatalogDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    /**
     * Default constructor; initializes the object.
     */
    RenderedCatalogDao() {
    }

    /**
     * Retrieves the entire rendered feed catalog as a list of RenderedFeedDiscoveryInfo objects.
     *
     * @return A list of RenderedFeedDiscoveryInfo objects representing the rendered feed catalog.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final List<RenderedFeedDiscoveryInfo> getCatalog() throws DataAccessException {
        try {
            HashOperations<String, Long, RenderedFeedDiscoveryInfo> hashOps = redisTemplate.opsForHash();
            return hashOps.values("RENDERED_CATALOG");
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "getCatalog", e.getMessage());
        }
    }

    /**
     * Updates the rendered feed catalog with the provided RenderedFeedDiscoveryInfo object.
     * If the object already exists in the catalog, it will be replaced.
     *
     * @param renderedFeedDiscoveryInfo The RenderedFeedDiscoveryInfo object to be updated or added to the catalog.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public final void update(RenderedFeedDiscoveryInfo renderedFeedDiscoveryInfo) throws DataAccessException {
        try {
            HashOperations<String, Long, RenderedFeedDiscoveryInfo> hashOps = redisTemplate.opsForHash();
            hashOps.put("RENDERED_CATALOG", renderedFeedDiscoveryInfo.getFeedDiscoveryInfo().getId(), renderedFeedDiscoveryInfo);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "updateCatalog", e.getMessage(), renderedFeedDiscoveryInfo);
        }
    }

    @Override
    public final String toString() {
        return "RenderedCatalogDao{" +
                "redisTemplate=" + redisTemplate +
                '}';
    }
}
