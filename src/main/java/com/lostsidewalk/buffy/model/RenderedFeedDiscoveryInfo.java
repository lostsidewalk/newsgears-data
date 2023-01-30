package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.discovery.ThumbnailedFeedDiscovery;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash(value = "feedDiscoveryInfo", timeToLive = 60 * 60 * 48) // TTL = 48h
public class RenderedFeedDiscoveryInfo implements Serializable {

    public static final long serialVersionUID = 409283401L;

    ThumbnailedFeedDiscovery feedDiscoveryInfo;

    private RenderedFeedDiscoveryInfo(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        this.feedDiscoveryInfo = feedDiscoveryInfo;
    }

    public static RenderedFeedDiscoveryInfo from(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        return new RenderedFeedDiscoveryInfo(feedDiscoveryInfo);
    }

    @SuppressWarnings("unused")
    public ThumbnailedFeedDiscovery getFeedDiscoveryInfo() {
        return feedDiscoveryInfo;
    }

    @SuppressWarnings("unused")
    public void setFeedDiscoveryInfo(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        this.feedDiscoveryInfo = feedDiscoveryInfo;
    }
}
