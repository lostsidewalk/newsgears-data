package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.model.RenderedFeedDiscoveryInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class RenderedCatalogDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    @SuppressWarnings("unused")
    public List<RenderedFeedDiscoveryInfo> getCatalog() throws DataAccessException {
        try {
            HashOperations<String, Long, RenderedFeedDiscoveryInfo> hashOps = this.redisTemplate.opsForHash();
            return hashOps.values("RENDERED_CATALOG");
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "getCatalog", e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    public void update(RenderedFeedDiscoveryInfo renderedFeedDiscoveryInfo) throws DataAccessException {
        try {
            HashOperations<String, Long, RenderedFeedDiscoveryInfo> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("RENDERED_CATALOG", renderedFeedDiscoveryInfo.getFeedDiscoveryInfo().getId(), renderedFeedDiscoveryInfo);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "updateCatalog", e.getMessage(), renderedFeedDiscoveryInfo);
        }
    }
}
