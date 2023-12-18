package com.lostsidewalk.buffy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 *
 */
@Slf4j
@Component
@Profile("redis")
public class LockDao {

    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    /**
     * Default constructor; initializes the object.
     */
    LockDao() {
    }

    //
    //
    //

    private static final int MAX_RETRIES = 3;
    private static final int LOCK_TIMEOUT_S = 30; // Lock expiration time in S
    private static final int RETRY_TIMEOUT_MS = 100; // Retry interval in MS

    /**
     * Acquires a lock on a resource with retry and error handling.
     *
     * @param lockKey   Unique identifier of the locked resource.
     * @param lockValue Unique identifier of the entity holding the lock.
     * @return True if the lock is successfully acquired, false if it fails after multiple retries.
     */
    @SuppressWarnings("unused")
    public final boolean acquireLockWithRetry(String lockKey, String lockValue) {
        log.debug("Attempting to acquire lock with retry, lockKey={}, lockValue={}", lockKey, lockValue);
        for (int retry = 0; MAX_RETRIES > retry; retry++) {
            boolean lockAcquired = acquireLock(lockKey, lockValue);
            if (lockAcquired) {
                log.debug("Redis lock acquired, lockKey={}, lockValue={}", lockKey, lockValue);
                return true;
            } else {
                log.debug("Failed to acquire redis lock, retry={}, , lockKey={}, lockValue={}", retry, lockKey, lockValue);
            }
            try {
                Thread.sleep(RETRY_TIMEOUT_MS);
            } catch (InterruptedException e) {
                log.info("Retry timeout exceeded, lockKey={}, lockValue={}", lockKey, lockValue);
                Thread.currentThread().interrupt();
            }
        }

        log.debug("Abandoning attempt to acquire lock, lockKey={}, lockValue={}", lockKey, lockValue);
        return false;
    }

    /**
     * Acquires a lock on a resource.
     *
     * @param lockKey   Unique identifier of the locked resource.
     * @param lockValue Unique identifier of the entity holding the lock.
     * @return True if the lock is successfully acquired, false if it fails after a single attempt.
     */
    @SuppressWarnings("unused")
    public final boolean acquireLock(String lockKey, String lockValue) {
        Boolean result = redisTemplate.opsForValue()
                .setIfAbsent(lockKey, lockValue, LOCK_TIMEOUT_S, SECONDS);
        return null != result && result;
    }

    private static final RedisScript<Long> releaseLockScript = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    /**
     * Releases a lock on a resource held by the given entity.
     *
     * @param lockKey   Unique identifier of the locked resource.
     * @param lockValue Unique identifier of the entity holding the lock.
     * @return True if the lock is successfully released, false otherwise.
     */
    @SuppressWarnings("unused")
    public final boolean releaseLock(String lockKey, String lockValue) {
        log.debug("Attempting to release lock, lockValue={}", lockValue);
        Long result = redisTemplate.execute(releaseLockScript, singletonList(lockKey), lockValue);
        boolean release = null != result && 1 == result;

        log.debug("Lock release result={}, lockKey={}, lockValue={}", result, lockKey, lockValue);
        return release;
    }

    @Override
    public final String toString() {
        return "LockDao{" +
                "redisTemplate=" + redisTemplate +
                '}';
    }
}
