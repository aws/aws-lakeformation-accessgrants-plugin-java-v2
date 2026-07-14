package software.amazon.lakeformation.plugin.accessgrants.cache;

import java.time.Duration;
import java.util.logging.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Cache for storing access denied exceptions.
 *
 * <p>Entries are stored at both the exact requested key AND the immediate parent directory.
 * This enables sibling deduplication: if one file in a folder is AccessDenied, all other
 * files in the same folder will hit the parent-level cache entry and avoid a redundant
 * Lake Formation call.
 *
 * <p>On lookup, the exact key is checked first (preserving existing behavior), then the
 * immediate parent is checked for sibling deduplication.
 */
public class AccessDeniedCache {

    private static final Logger LOGGER = Logger.getLogger(AccessDeniedCache.class.getName());

    private static final int ACCESS_DENIED_CACHE_SIZE = 3000;
    private static final int ACCESS_DENIED_CACHE_TTL = 5 * 60; // 5 minutes in seconds
    private static final String S3_SCHEME = "s3://";

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

    /**
     * Stores the exception at both the exact key and the immediate parent directory.
     * The parent-level entry enables sibling deduplication: subsequent requests for
     * different files in the same folder will hit the parent entry without calling
     * Lake Formation again.
     *
     * @param cacheKey the cache key for the denied request
     * @param e the exception to cache
     */
    public void putValueInCache(final CacheKey cacheKey, final Exception e) {
        // Store at exact key (existing behavior)
        accessDeniedCache.put(cacheKey, e);

        // Also store at immediate parent for sibling deduplication
        String parentPrefix = immediateParent(cacheKey.getS3Prefix());
        if (parentPrefix != null) {
            CacheKey parentKey = new CacheKey(
                cacheKey.getCredentials(), cacheKey.getPermission(), parentPrefix);
            accessDeniedCache.put(parentKey, e);
        }
    }

    /**
     * Checks for a cached exception, first at the exact key, then at the immediate parent.
     * The parent check enables sibling deduplication: if any file in a folder was denied,
     * all siblings will find the parent-level entry.
     *
     * @param cacheKey the cache key to look up
     * @return the cached exception, or null if not found
     */
    public Exception getValueFromCache(final CacheKey cacheKey) {
        // Check exact key first
        Exception cached = accessDeniedCache.getIfPresent(cacheKey);
        if (cached != null) {
            return cached;
        }

        // Check immediate parent for sibling deduplication
        String parentPrefix = immediateParent(cacheKey.getS3Prefix());
        if (parentPrefix != null) {
            CacheKey parentKey = new CacheKey(
                cacheKey.getCredentials(), cacheKey.getPermission(), parentPrefix);
            return accessDeniedCache.getIfPresent(parentKey);
        }
        return null;
    }

    /**
     * Returns the immediate parent directory of an S3 path, or null if no valid parent exists.
     *
     * <p>If the path ends with "/*" or "/", it is a directory reference and the normalized form
     * IS the parent directory. Otherwise, the leaf segment (file name) is dropped.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code s3://bucket/f1/f2/file.csv} returns {@code s3://bucket/f1/f2}</li>
     *   <li>{@code s3://bucket/folder/} returns {@code s3://bucket/folder}</li>
     *   <li>{@code s3://bucket/folder/*} returns {@code s3://bucket/folder}</li>
     *   <li>{@code s3://bucket/file.csv} returns {@code s3://bucket}</li>
     *   <li>{@code s3://bucket} returns null (no parent above bucket root)</li>
     * </ul>
     *
     * @param s3Prefix the full S3 path
     * @return the immediate parent directory, or null if no valid parent exists
     */
    private String immediateParent(final String s3Prefix) {
        if (s3Prefix == null) {
            return null;
        }

        String normalized = s3Prefix;
        if (normalized.endsWith("/*")) {
            normalized = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        if (!normalized.startsWith(S3_SCHEME)) {
            return null;
        }

        int bucketRootEnd = normalized.indexOf('/', S3_SCHEME.length());
        if (bucketRootEnd == -1) {
            return null;
        }

        // If original ended with / or /*, normalized IS the parent
        if (s3Prefix.endsWith("/*") || s3Prefix.endsWith("/")) {
            return normalized;
        }

        // Drop last segment
        int lastSlash = normalized.lastIndexOf('/');
        if (lastSlash > bucketRootEnd) {
            return normalized.substring(0, lastSlash);
        }
        return normalized.substring(0, bucketRootEnd);
    }
}
