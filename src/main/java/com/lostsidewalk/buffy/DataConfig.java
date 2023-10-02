package com.lostsidewalk.buffy;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Configuration class for setting up Redis data storage and access.
 * This class is activated when the "redis" profile is active.
 */
@SuppressWarnings("deprecation")
@Profile("redis")
@Configuration
public class DataConfig {

    /**
     * The Redis server host name obtained from configuration.
     */
    @Value("${spring.redis.host}")
    String redisHostName;

    /**
     * The Redis server password obtained from configuration.
     */
    @Value("${spring.redis.password}")
    String redisPassword;

    /**
     * The Redis server port, with a default value of 6379.
     */
    @Value("${spring.redis.port:6379}")
    int redisPort;

    /**
     * Default constructor; initializes the object.
     */
    DataConfig() {
        super();
    }

    /**
     * Configures and returns a JedisConnectionFactory bean for connecting to the Redis server.
     *
     * @return A JedisConnectionFactory instance.
     */
    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
        connectionFactory.setHostName(redisHostName);
        connectionFactory.setPassword(redisPassword);
        connectionFactory.setPort(redisPort);

        return connectionFactory;
    }

    /**
     * Configures and returns a RedisTemplate bean for interacting with Redis data.
     *
     * @return A RedisTemplate instance.
     */
    @Bean
    RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> rssFeedTemplate = new RedisTemplate<>();
        rssFeedTemplate.setConnectionFactory(jedisConnectionFactory());
        return rssFeedTemplate;
    }
}
