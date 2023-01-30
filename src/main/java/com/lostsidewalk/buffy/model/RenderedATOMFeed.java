package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.atom.Feed;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash(value = "atomFeed")
public class RenderedATOMFeed implements Serializable {

    public static final long serialVersionUID = 5882302L;

    String transportIdent;

    Feed feed;

    private RenderedATOMFeed(String transportIdent, Feed feed) {
        this.transportIdent = transportIdent;
        this.feed = feed;
    }

    @SuppressWarnings("unused")
    public static RenderedATOMFeed from(String transportIdent, Feed feed) {
        return new RenderedATOMFeed(transportIdent, feed);
    }

    @SuppressWarnings("unused")
    public String getTransportIdent() {
        return transportIdent;
    }

    @SuppressWarnings("unused")
    public void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    @SuppressWarnings("unused")
    public Feed getFeed() {
        return feed;
    }

    @SuppressWarnings("unused")
    public void setFeed(Feed feed) {
        this.feed = feed;
    }
}
