package com.lostsidewalk.buffy;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class DataConfig {

    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
        connectionFactory.setHostName("redis");
        connectionFactory.setPassword("redis");

        return connectionFactory;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> rssFeedTemplate = new RedisTemplate<>();
        rssFeedTemplate.setConnectionFactory(jedisConnectionFactory());
        return rssFeedTemplate;
    }
}
