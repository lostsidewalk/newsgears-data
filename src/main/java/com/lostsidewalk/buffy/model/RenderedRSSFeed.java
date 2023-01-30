package com.lostsidewalk.buffy.model;

import com.rometools.rome.feed.rss.Channel;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash(value = "rssFeed")
public class RenderedRSSFeed implements Serializable {

    public static final long serialVersionUID = 5882300L;

    String transportIdent;

    Channel channel;

    private RenderedRSSFeed(String transportIdent, Channel channel) {
        this.transportIdent = transportIdent;
        this.channel = channel;
    }

    @SuppressWarnings("unused")
    public static RenderedRSSFeed from(String transportIdent, Channel channel) {
        return new RenderedRSSFeed(transportIdent, channel);
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
    public Channel getChannel() {
        return channel;
    }

    @SuppressWarnings("unused")
    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
