package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.DataAccessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

/**
 * The RenderedThumbnailDao class is responsible for accessing and managing rendered thumbnail images
 * stored in Redis. It provides methods for retrieving and storing thumbnail images associated with
 * specific transport identifiers.
 */
@Slf4j
@Component
@Profile("redis")
public class RenderedThumbnailDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    /**
     * Retrieves a rendered thumbnail image from Redis based on its transport identifier.
     *
     * @param transportIdent The transport identifier associated with the thumbnail to retrieve.
     * @return The rendered thumbnail image as a RenderedThumbnail object.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public RenderedThumbnail findThumbnailByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedThumbnail> hashOps = this.redisTemplate.opsForHash();
            return hashOps.get("THUMBNAILS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "findThumbnailByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    /**
     * Stores a rendered thumbnail image in Redis with the specified transport identifier.
     *
     * @param transportIdent The transport identifier associated with the thumbnail.
     * @param thumbnail      The RenderedThumbnail object representing the thumbnail image to store.
     * @throws DataAccessException If an error occurs while accessing the data.
     */
    @SuppressWarnings("unused")
    public void putThumbnailAtTransportIdent(String transportIdent, RenderedThumbnail thumbnail) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedThumbnail> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("THUMBNAILS", transportIdent, thumbnail);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage());
            throw new DataAccessException(getClass().getSimpleName(), "putThumbnailAtTransportIdent", e.getMessage(), transportIdent);
        }
    }
}
