package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.atom.Feed;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash("atomFeed")
public class RenderedATOMFeed implements Serializable {

    public static final long serialVersionUID = 5882302L;

    String transportIdent;

    Feed feed;

    private RenderedATOMFeed(String transportIdent, Feed feed) {
        this.transportIdent = transportIdent;
        this.feed = feed;
    }

    public static RenderedATOMFeed from(String transportIdent, Feed feed) {
        return new RenderedATOMFeed(transportIdent, feed);
    }

    public String getTransportIdent() {
        return transportIdent;
    }

    public void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    public Feed getFeed() {
        return feed;
    }

    public void setFeed(Feed feed) {
        this.feed = feed;
    }
}
