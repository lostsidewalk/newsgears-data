package com.lostsidewalk.buffy.model;

import org.springframework.data.redis.core.RedisHash;

import java.io.Serializable;

@RedisHash(value = "thumbnail", timeToLive = 60 * 60 * 48) // TTL = 48h
public class RenderedThumbnail implements Serializable {

    public static final long serialVersionUID = 1842305L;

    String transportIdent;

    byte[] image;

    private RenderedThumbnail(String transportIdent, byte[] image) {
        this.transportIdent = transportIdent;
        this.image = image;
    }

    @SuppressWarnings("unused")
    public static RenderedThumbnail from(String transportIdent, byte[] image) {
        return new RenderedThumbnail(transportIdent, image);
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
    public byte[] getImage() {
        return image;
    }

    @SuppressWarnings("unused")
    public void setImage(byte[] image) {
        this.image = image;
    }
}
