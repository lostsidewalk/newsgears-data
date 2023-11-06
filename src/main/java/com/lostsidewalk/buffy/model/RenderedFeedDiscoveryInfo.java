package com.lostsidewalk.buffy.model;

import com.lostsidewalk.buffy.discovery.ThumbnailedFeedDiscovery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serial;
import java.io.Serializable;

/**
 * The RenderedFeedDiscoveryInfo class represents a serializable wrapper for ThumbnailedFeedDiscovery objects.
 * It is used for storing feed discovery information in Redis with a specified time-to-live (TTL) duration.
 *
 * @see ThumbnailedFeedDiscovery
 */
@Slf4j
@RedisHash(value = "feedDiscoveryInfo", timeToLive = (60 * 60 * 48)) // TTL = 48 hours
public class RenderedFeedDiscoveryInfo implements Serializable {

    @Serial
    private static final long serialVersionUID = 409283401L;

    /**
     * The ThumbnailedFeedDiscovery object wrapped by this RenderedFeedDiscoveryInfo instance.
     */
    private ThumbnailedFeedDiscovery feedDiscoveryInfo;

    private RenderedFeedDiscoveryInfo(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        this.feedDiscoveryInfo = feedDiscoveryInfo;
    }

    /**
     * Creates a new RenderedFeedDiscoveryInfo instance from a ThumbnailedFeedDiscovery object.
     *
     * @param feedDiscoveryInfo The ThumbnailedFeedDiscovery object to wrap.
     * @return A new RenderedFeedDiscoveryInfo instance.
     */
    public static RenderedFeedDiscoveryInfo from(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        return new RenderedFeedDiscoveryInfo(feedDiscoveryInfo);
    }

    /**
     * Gets the wrapped ThumbnailedFeedDiscovery object.
     *
     * @return The ThumbnailedFeedDiscovery object.
     */
    @SuppressWarnings("unused")
    public final ThumbnailedFeedDiscovery getFeedDiscoveryInfo() {
        return feedDiscoveryInfo;
    }

    /**
     * Sets the ThumbnailedFeedDiscovery object to be wrapped by this RenderedFeedDiscoveryInfo instance.
     *
     * @param feedDiscoveryInfo The ThumbnailedFeedDiscovery object to set.
     */
    @SuppressWarnings("unused")
    public final void setFeedDiscoveryInfo(ThumbnailedFeedDiscovery feedDiscoveryInfo) {
        this.feedDiscoveryInfo = feedDiscoveryInfo;
    }

    @Override
    public final String toString() {
        return "RenderedFeedDiscoveryInfo{" +
                "feedDiscoveryInfo=" + feedDiscoveryInfo +
                '}';
    }
}
