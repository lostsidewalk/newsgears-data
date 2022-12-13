package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.model.RenderedATOMFeed;
import com.lostsidewalk.buffy.model.RenderedRSSFeed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Slf4j
@Component
public class RenderedFeedDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;
    //
    // RSS feed methods
    //
    @SuppressWarnings("unused")
    public RenderedRSSFeed findRSSChannelByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedRSSFeed> hashOps = this.redisTemplate.opsForHash();
            return hashOps.get("RENDERED_RSS_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findRSSChannelByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    @SuppressWarnings("unused")
    public void putRSSFeedAtTransportIdent(String transportIdent, RenderedRSSFeed renderedRSSFeed) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedRSSFeed> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("RENDERED_RSS_FEEDS", transportIdent, renderedRSSFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "putRSSFeedAtTransportIdent", e.getMessage(), transportIdent, renderedRSSFeed);
        }
    }
    //
    // ATOM feed methods
    //
    @SuppressWarnings("unused")
    public RenderedATOMFeed findATOMFeedByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedATOMFeed> hashOps = this.redisTemplate.opsForHash();
            return hashOps.get("RENDERED_ATOM_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findATOMFeedByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    @SuppressWarnings("unused")
    public void putATOMFeedAtTransportIdent(String transportIdent, RenderedATOMFeed renderedATOMFeed) throws DataAccessException {
        try {
            HashOperations<String, String, RenderedATOMFeed> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("RENDERED_ATOM_FEEDS", transportIdent, renderedATOMFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "putATOMFeedAtTransportIdent", e.getMessage(), transportIdent, renderedATOMFeed);
        }
    }
    // 
    // JSON feed methods 
    // 
    @SuppressWarnings("unused")
    public <T extends Serializable> T findJSONFeedByTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, T> hashOps = this.redisTemplate.opsForHash();
            return hashOps.get("RENDERED_JSON_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "findJSONFeedByTransportIdent", e.getMessage(), transportIdent);
        }
    }

    @SuppressWarnings("unused")
    public <T extends Serializable> void putJSONFeedAtTransportIdent(String transportIdent, T renderedJSONFeed) throws DataAccessException {
        try {
            HashOperations<String, String, T> hashOps = this.redisTemplate.opsForHash();
            hashOps.put("RENDERED_JSON_FEEDS", transportIdent, renderedJSONFeed);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "putJSONFeedAtTransportIdent", e.getMessage(), transportIdent, renderedJSONFeed);
        }
    }
    //
    //
    //
    @SuppressWarnings("unused")
    public void deleteFeedAtTransportIdent(String transportIdent) throws DataAccessException {
        try {
            HashOperations<String, String, Object> hashOps = this.redisTemplate.opsForHash();
            hashOps.delete("RENDERED_RSS_FEEDS", transportIdent);
            hashOps.delete("RENDERED_ATOM_FEEDS", transportIdent);
            hashOps.delete("RENDERED_JSON_FEEDS", transportIdent);
        } catch (Exception e) {
            log.error("Something horrible happened due to: {}", e.getMessage(), e);
            throw new DataAccessException(getClass().getSimpleName(), "deleteFeedAtTransportIdent", e.getMessage(), transportIdent);
        }
    }
}
