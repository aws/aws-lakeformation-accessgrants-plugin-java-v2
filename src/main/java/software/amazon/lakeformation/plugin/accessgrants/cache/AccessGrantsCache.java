package software.amazon.lakeformation.plugin.accessgrants.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;

import java.time.Duration;

/**
 * Cache for storing access grants credentials.
 */
public class AccessGrantsCache {
    private static final int DEFAULT_ACCESS_GRANTS_CACHE_SIZE = 30000;
    private static final int MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE = 1000000;
    private static final int GET_DATA_ACCESS_DURATION = 1 * 60 * 60; // 1 hour
    private static final int MAX_GET_DATA_ACCESS_DURATION = 12 * 60 * 60; // 12 hours
    private static final int CACHE_EXPIRATION_TIME_PERCENTAGE = 90;

    private final Cache<CacheKey, AwsCredentials> accessGrantsCache;

    public AccessGrantsCache() {
        this(DEFAULT_ACCESS_GRANTS_CACHE_SIZE, GET_DATA_ACCESS_DURATION);
    }

    public AccessGrantsCache(final int cacheSize, final int duration) {
        if (cacheSize > MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE) {
            throw new IllegalArgumentException(
                "Max cache size should be less than or equal to " + MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE);
        }

        if (duration > MAX_GET_DATA_ACCESS_DURATION) {
            throw new IllegalArgumentException(
                "Maximum duration should be less than or equal to " + MAX_GET_DATA_ACCESS_DURATION);
        }

        long cacheTtl = ((long) duration * CACHE_EXPIRATION_TIME_PERCENTAGE) / 100;
        this.accessGrantsCache = Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(Duration.ofSeconds(cacheTtl))
            .build();
    }

    public AwsCredentials getCredentials(
            final LakeFormationClient lfClient,
            final CacheKey cacheKey,
            final AccessDeniedCache accessDeniedCache) {
        // TODO: Temporarily throw UnsupportedOperationException until Lake Formation API classes are available
        throw new UnsupportedOperationException("Lake Formation API integration not yet available - missing SDK classes");
    }
}
