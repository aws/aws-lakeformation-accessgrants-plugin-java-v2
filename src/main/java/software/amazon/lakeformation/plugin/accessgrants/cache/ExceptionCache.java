package software.amazon.lakeformation.plugin.accessgrants.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import software.amazon.awssdk.services.lakeformation.model.ConflictException;
import software.amazon.awssdk.services.lakeformation.model.EntityNotFoundException;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Negative cache for non-retryable Lake Formation exceptions.
 *
 * <p>These errors are driven by the Lake Formation registration topology of an S3 location,
 * not by an individual object, and the plugin has no way of knowing which registered ancestor prefix
 * the failure applies to.
 *
 * <p>Because of that, an entry is stored for <em>every parent prefix</em> of the failed object, from
 * its immediate parent directory up to the bucket root, and lookups walk the same parent prefixes.
 *
 */
public class ExceptionCache {
    private static final Logger LOGGER = Logger.getLogger(ExceptionCache.class.getName());

    private static final int EXCEPTION_CACHE_SIZE = 10000;
    private static final int EXCEPTION_CACHE_TTL = 3 * 60; // 3 minutes in seconds
    private static final String S3_SCHEME = "s3://";

    /**
     * Negative-cache entries are permission-agnostic. Conflict / EntityNotFound errors reflect how a
     * location is registered in Lake Formation, independent of the READ / WRITE / READWRITE scope of
     * the request, so every permission is normalized to this single sentinel when building keys. This
     * lets a WRITE failure suppress a subsequent READ request (and vice versa) for the same prefix.
     */
    private static final Permission NEGATIVE_CACHE_PERMISSION = Permission.READWRITE;

    private final Cache<CacheKey, LakeFormationException> exceptionCache;

    public ExceptionCache() {
        this(EXCEPTION_CACHE_SIZE, EXCEPTION_CACHE_TTL);
    }

    public ExceptionCache(final int cacheSize, final int ttl) {
        this.exceptionCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(Duration.ofSeconds(ttl))
                .build();
    }

    /**
     * @return true when the exception should be remembered by the negative cache. Only the
     *     non-retryable, registration-driven Lake Formation errors qualify; retryable errors (e.g.
     *     ThrottledException) and AccessDenied (handled by {@link AccessDeniedCache}) must not.
     */
    public static boolean isNegativeCacheable(final LakeFormationException e) {
        return e instanceof ConflictException || e instanceof EntityNotFoundException;
    }

    /**
     * Caches the exception for every parent prefix of the requested key, from the failed object's
     * immediate parent directory up to (and including) the bucket root.
     */
    public void cacheForAllPrefixes(final CacheKey cacheKey, final LakeFormationException e) {
        final List<String> prefixes = ancestorPrefixesUpToBucket(cacheKey.getS3Prefix());
        LOGGER.info("Caching negative response for " + prefixes.size() + " parent prefixes of: "
            + cacheKey.getS3Prefix());
        for (final String prefix : prefixes) {
            LOGGER.info("Caching key: " + prefix);
            exceptionCache.put(negativeKey(cacheKey, prefix), e);
        }
    }

    /**
     * Walks the parent prefixes of the requested key and returns the first cached exception found, or
     * null if none of the ancestors have a negative entry. The same parent-prefix walk is used on
     * store and lookup, so a failure recorded for one object is found for any sibling that shares an
     * ancestor prefix.
     */
    public LakeFormationException getIfPrefixCached(final CacheKey cacheKey) {
        for (final String prefix : ancestorPrefixesUpToBucket(cacheKey.getS3Prefix())) {
            final LakeFormationException cached = exceptionCache.getIfPresent(negativeKey(cacheKey, prefix));
            if (cached != null) {
                LOGGER.info("Found cached negative response at prefix: " + prefix);
                return cached;
            }
        }
        return null;
    }

    /**
     * Builds a permission-agnostic negative-cache key for a given prefix
     */
    private CacheKey negativeKey(final CacheKey cacheKey, final String prefix) {
        return new CacheKey(cacheKey.getCredentials(), NEGATIVE_CACHE_PERMISSION, prefix);
    }

    /**
     * Builds the ordered list of ancestor prefixes for an S3 key, from the immediate parent directory
     * up to and including the bucket root ("s3://bucket").
     *
     * The leaf segment (the object name) is dropped so that per-object keys never bloat the cache.
     * Any trailing "/" or "/*" is normalized away first so store and lookup keys are consistent.
     */
    private List<String> ancestorPrefixesUpToBucket(final String s3Prefix) {
        final List<String> prefixes = new ArrayList<>();
        String prefix = normalize(s3Prefix);
        if (!prefix.startsWith(S3_SCHEME)) {
            // Unexpected format - cache nothing
            return prefixes;
        }

        // First "/" after the scheme separates the bucket from the key.
        final int bucketRootEnd = prefix.indexOf('/', S3_SCHEME.length());
        if (bucketRootEnd == -1) {
            // Only a bucket root was supplied; there is nothing above it to cache.
            prefixes.add(prefix);
            return prefixes;
        }

        final String bucketRoot = prefix.substring(0, bucketRootEnd);
        // Drop the leaf segment, then walk parents up to the bucket root.
        int lastSlash = prefix.lastIndexOf('/');
        while (lastSlash > bucketRootEnd) {
            prefix = prefix.substring(0, lastSlash);
            prefixes.add(prefix);
            lastSlash = prefix.lastIndexOf('/');
        }
        prefixes.add(bucketRoot);
        return prefixes;
    }

    /**
     * Strips a trailing "/*" or "/" from a prefix so directory / wildcard requests produce the same
     * keys as object requests under the same location. Mirrors the normalization done for the
     * positive cache when processing a matched grant target.
     */
    private String normalize(final String prefix) {
        if (prefix.endsWith("/*")) {
            return prefix.substring(0, prefix.length() - 2);
        } else if (prefix.endsWith("/")) {
            return prefix.substring(0, prefix.length() - 1);
        }
        return prefix;
    }
}
