package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.rss.Channel;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash("rssFeed")
public class RenderedRSSFeed implements Serializable {

    public static final long serialVersionUID = 5882300L;

    String transportIdent;

    Channel channel;

    private RenderedRSSFeed(String transportIdent, Channel channel) {
        this.transportIdent = transportIdent;
        this.channel = channel;
    }

    public static RenderedRSSFeed from(String transportIdent, Channel channel) {
        return new RenderedRSSFeed(transportIdent, channel);
    }

    public String getTransportIdent() {
        return transportIdent;
    }

    public void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
