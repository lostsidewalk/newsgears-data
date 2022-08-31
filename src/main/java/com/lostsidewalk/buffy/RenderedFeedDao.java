package com.lostsidewalk.buffy;

import com.lostsidewalk.buffy.model.RenderedATOMFeed;
import com.lostsidewalk.buffy.model.RenderedRSSFeed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RenderedFeedDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;
    //
    // RSS feed methods
    //
    @SuppressWarnings("unused")
    public RenderedRSSFeed findRSSChannelByTransportIdent(String transportIdent) {
        HashOperations<String, String, RenderedRSSFeed> hashOps = this.redisTemplate.opsForHash();
        return hashOps.get("RENDERED_RSS_FEEDS", transportIdent);
    }

    public void putRSSFeedAtTransportIdent(String transportIdent, RenderedRSSFeed renderedRSSFeed) {
        HashOperations<String, String, RenderedRSSFeed> hashOps = this.redisTemplate.opsForHash();
        hashOps.put("RENDERED_RSS_FEEDS", transportIdent, renderedRSSFeed);
    }
    //
    // ATOM feed methods
    //
    @SuppressWarnings("unused")
    public RenderedATOMFeed findATOMFeedByTransportIdent(String transportIdent) {
        HashOperations<String, String, RenderedATOMFeed> hashOps = this.redisTemplate.opsForHash();
        return hashOps.get("RENDERED_ATOM_FEEDS", transportIdent);
    }

    @SuppressWarnings("unused")
    public void putATOMFeedAtTransportIdent(String transportIdent, RenderedATOMFeed renderedATOMFeed) {
        HashOperations<String, String, RenderedATOMFeed> hashOps = this.redisTemplate.opsForHash();
        hashOps.put("RENDERED_ATOM_FEEDS", transportIdent, renderedATOMFeed);
    }
}
