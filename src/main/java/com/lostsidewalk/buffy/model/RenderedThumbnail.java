package com.lostsidewalk.buffy.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisHash;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

/**
 * The RenderedThumbnail class represents a serializable wrapper for thumbnail images.
 * It is used for storing rendered thumbnail images in Redis with a specified time-to-live (TTL).
 */
@Slf4j
@RedisHash(value = "thumbnail", timeToLive = (60 * 60 * 48)) // TTL = 48 hours
public class RenderedThumbnail implements Serializable {

    @Serial
    private static final long serialVersionUID = 1842305L;

    /**
     * The transport identifier associated with the thumbnail.
     */
    private String transportIdent;

    /**
     * The byte array representing the thumbnail image.
     */
    private byte[] image;

    private RenderedThumbnail(String transportIdent, byte[] image) {
        this.transportIdent = transportIdent;
        this.image = image;
    }

    /**
     * Creates a new RenderedThumbnail instance from a transport identifier and the image data.
     *
     * @param transportIdent The transport identifier associated with the thumbnail.
     * @param image          The byte array representing the thumbnail image.
     * @return A new RenderedThumbnail instance.
     */
    @SuppressWarnings("unused")
    public static RenderedThumbnail from(String transportIdent, byte[] image) {
        return new RenderedThumbnail(transportIdent, image);
    }

    /**
     * Gets the transport identifier associated with the thumbnail.
     *
     * @return The transport identifier.
     */
    @SuppressWarnings("unused")
    public final String getTransportIdent() {
        return transportIdent;
    }

    /**
     * Sets the transport identifier associated with the thumbnail.
     *
     * @param transportIdent The transport identifier to set.
     */
    @SuppressWarnings("unused")
    public final void setTransportIdent(String transportIdent) {
        this.transportIdent = transportIdent;
    }

    /**
     * Gets the byte array representing the thumbnail image.
     *
     * @return The thumbnail image data as a byte array.
     */
    @SuppressWarnings("unused")
    public final byte[] getImage() {
        return image;
    }

    /**
     * Sets the byte array representing the thumbnail image.
     *
     * @param image The thumbnail image data as a byte array to set.
     */
    @SuppressWarnings("unused")
    public final void setImage(byte[] image) {
        this.image = image;
    }

    @Override
    public final String toString() {
        return "RenderedThumbnail{" +
                "transportIdent='" + transportIdent + '\'' +
                ", image=" + Arrays.toString(image) +
                '}';
    }
}
