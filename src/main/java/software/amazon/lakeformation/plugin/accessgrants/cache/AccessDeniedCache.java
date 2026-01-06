package software.amazon.lakeformation.plugin.accessgrants.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;

/**
 * Cache for storing access denied exceptions.
 */
public class AccessDeniedCache {
    private static final int ACCESS_DENIED_CACHE_SIZE = 3000;
    private static final int ACCESS_DENIED_CACHE_TTL = 5 * 60; // 5 minutes in seconds

    private final Cache<CacheKey, Exception> accessDeniedCache;

    public AccessDeniedCache() {
        this(ACCESS_DENIED_CACHE_SIZE, ACCESS_DENIED_CACHE_TTL);
    }

    public AccessDeniedCache(int cacheSize, int ttl) {
        this.accessDeniedCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(Duration.ofSeconds(ttl))
                .build();
    }

    public void putValueInCache(CacheKey key, Exception value) {
        accessDeniedCache.put(key, value);
    }

    public Exception getValueFromCache(CacheKey key) {
        return accessDeniedCache.getIfPresent(key);
    }
}
