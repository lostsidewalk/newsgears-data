package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.model.Thumbnail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ThumbnailDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    @SuppressWarnings("unused")
    public Thumbnail findThumbnailByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, Thumbnail> hashOps = this.redisTemplate.opsForHash();
            return hashOps.get("THUMBNAILS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findThumbnailByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    @SuppressWarnings("unused")
    public void putThumbnailAtTransportIdent(String transportIdent, Thumbnail thumbnail) throws DataAccessException {
        try {
            HashOperations<String, String, Thumbnail> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("THUMBNAILS", transportIdent, thumbnail);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "putThumbnailAtTransportIdent", e.getMessage(), transportIdent);
        }
    }
}
