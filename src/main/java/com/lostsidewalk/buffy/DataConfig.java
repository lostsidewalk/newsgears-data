package com.lostsidewalk.buffy;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class DataConfig {

    @Value("${spring.redis.host}")
    String redisHostName;

    @Value("${spring.redis.password}")
    String redisPassword;

    @Value("${spring.redis.port:6379}")
    int redisPort;

    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
        connectionFactory.setHostName(redisHostName);
        connectionFactory.setPassword(redisPassword);
        connectionFactory.setPort(redisPort);

        return connectionFactory;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> rssFeedTemplate = new RedisTemplate<>();
        rssFeedTemplate.setConnectionFactory(jedisConnectionFactory());
        return rssFeedTemplate;
    }
}
