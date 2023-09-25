package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.atom.Feed;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serial;
import java.io.Serializable;

/**
 * Represents a Rendered Atom Feed along with its associated transport identifier.
 * This class is used to store and retrieve Atom Feeds in Redis.
 */
@RedisHash(value = "atomFeed")
public class RenderedATOMFeed implements Serializable {

    @Serial
    private static final long serialVersionUID = 5882302L;

    /**
     * The transport identifier associated with this RenderedATOMFeed.
     */
    String transportIdent;

    /**
     * The Atom Feed stored in this RenderedATOMFeed.
     */
    Feed feed;

    private RenderedATOMFeed(String transportIdent, Feed feed) {
        this.transportIdent = transportIdent;
        this.feed = feed;
    }

    /**
     * Creates a new instance of RenderedATOMFeed with the specified transport identifier and Feed.
     *
     * @param transportIdent The transport identifier associated with the Atom Feed.
     * @param feed The Atom Feed to be stored.
     * @return A new RenderedATOMFeed instance.
     */
    @SuppressWarnings("unused")
    public static RenderedATOMFeed from(String transportIdent, Feed feed) {
        return new RenderedATOMFeed(transportIdent, feed);
    }

    /**
     * Gets the transport identifier associated with this RenderedATOMFeed.
     *
     * @return The transport identifier.
     */
    @SuppressWarnings("unused")
    public String getTransportIdent() {
        return transportIdent;
    }

    /**
     * Sets the transport identifier associated with this RenderedATOMFeed.
     *
     * @param transportIdent The transport identifier to set.
     */
    @SuppressWarnings("unused")
    public void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    /**
     * Gets the Atom Feed stored in this RenderedATOMFeed.
     *
     * @return The Atom Feed.
     */
    @SuppressWarnings("unused")
    public Feed getFeed() {
        return feed;
    }

    /**
     * Sets the Atom Feed to be stored in this RenderedATOMFeed.
     *
     * @param feed The Atom Feed to set.
     */
    @SuppressWarnings("unused")
    public void setFeed(Feed feed) {
        this.feed = feed;
    }
}
