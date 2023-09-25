package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.rss.Channel;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serial;
import java.io.Serializable;

/**
 * The RenderedRSSFeed class represents a serializable wrapper for RSS (Really Simple Syndication) feed Channel objects.
 * It is used for storing rendered RSS feed data in Redis.
 *
 * @see com.rometools.rome.feed.rss.Channel
 */
@RedisHash(value = "rssFeed")
public class RenderedRSSFeed implements Serializable {

    @Serial
    private static final long serialVersionUID = 5882300L;

    /**
     * The transport identifier associated with this RenderedRSSFeed.
     */
    private String transportIdent;

    /**
     * The RSS Channel stored in this RenderedRSSFeed.
     */
    private Channel channel;

    private RenderedRSSFeed(String transportIdent, Channel channel) {
        this.transportIdent = transportIdent;
        this.channel = channel;
    }

    /**
     * Creates a new RenderedRSSFeed instance from a transport identifier and a Channel object.
     *
     * @param transportIdent The transport identifier for the RSS feed.
     * @param channel        The Channel object representing the RSS feed.
     * @return A new RenderedRSSFeed instance.
     */
    @SuppressWarnings("unused")
    public static RenderedRSSFeed from(String transportIdent, Channel channel) {
        return new RenderedRSSFeed(transportIdent, channel);
    }

    /**
     * Gets the transport identifier for the RSS feed.
     *
     * @return The transport identifier.
     */
    @SuppressWarnings("unused")
    public String getTransportIdent() {
        return transportIdent;
    }

    /**
     * Sets the transport identifier for the RSS feed.
     *
     * @param transportIdent The transport identifier to set.
     */
    @SuppressWarnings("unused")
    public void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    /**
     * Gets the Channel object representing the RSS feed.
     *
     * @return The Channel object.
     */
    @SuppressWarnings("unused")
    public Channel getChannel() {
        return channel;
    }

    /**
     * Sets the Channel object representing the RSS feed.
     *
     * @param channel The Channel object to set.
     */
    @SuppressWarnings("unused")
    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
