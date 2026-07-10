package software.amazon.lakeformation.plugin.accessgrants.cache;

import java.time.Duration;
import java.util.Optional;
import java.util.logging.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import software.amazon.awssdk.services.lakeformation.model.ConflictException;
import software.amazon.awssdk.services.lakeformation.model.EntityNotFoundException;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.s3control.model.Permission;

/**
 * Negative cache for non-retryable Lake Formation exceptions.
 *
 * <p>These errors are driven by the Lake Formation registration topology of an S3 location.
 * An entry is stored only at the <em>immediate parent directory</em> of the failed object path.
 * This prevents a single failure from poisoning unrelated paths in the same bucket while still
 * suppressing repeated calls to sibling objects under the same folder.
 *
 * <p>Example: a failure at {@code s3://bucket/folder1/folder2/file.csv} caches only at
 * {@code s3://bucket/folder1/folder2}. Sibling objects like
 * {@code s3://bucket/folder1/folder2/other.csv} share the same parent and will be
 * short-circuited, but unrelated paths like {@code s3://bucket/folder1/otherFolder/file.csv}
 * will NOT be affected.
 */
public class ExceptionCache {
    private static final Logger LOGGER = Logger.getLogger(ExceptionCache.class.getName());

    private static final int EXCEPTION_CACHE_SIZE = 10000;
    private static final int EXCEPTION_CACHE_TTL = 3 * 60; // 3 minutes in seconds
    private static final String S3_SCHEME = "s3://";

    /**
     * Negative-cache entries are permission-agnostic. Conflict / EntityNotFound errors reflect how
     * a location is registered in Lake Formation, independent of the READ / WRITE / READWRITE
     * scope of the request, so every permission is normalized to this single sentinel when building
     * keys. This lets a WRITE failure suppress a subsequent READ request (and vice versa) for the
     * same prefix.
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
     * Returns true when the exception should be remembered by the negative cache. Only the
     * non-retryable, registration-driven Lake Formation errors qualify; retryable errors (e.g.
     * ThrottledException) and AccessDenied (handled by {@link AccessDeniedCache}) must not.
     */
    public static boolean isNegativeCacheable(final LakeFormationException e) {
        return e instanceof ConflictException || e instanceof EntityNotFoundException;
    }

    /**
     * Caches the exception at the immediate parent directory of the requested key only.
     *
     * <p>For example, given {@code s3://bucket/folder1/folder2/file.csv}, the entry is cached
     * at {@code s3://bucket/folder1/folder2}. This prevents cross-contamination between
     * unrelated table paths while still suppressing repeated calls to sibling objects
     * under the same folder.
     *
     * @param cacheKey the original request cache key (with full object path)
     * @param e the non-retryable exception to cache
     */
    public void cacheForImmediateParent(final CacheKey cacheKey,
                                        final LakeFormationException e) {
        final Optional<String> parent = immediateParent(cacheKey.getS3Prefix());
        if (parent.isPresent()) {
            LOGGER.info("Caching negative response at immediate parent: " + parent.get()
                + " for path: " + cacheKey.getS3Prefix());
            exceptionCache.put(negativeKey(cacheKey, parent.get()), e);
        } else {
            LOGGER.info("No cacheable parent found for path: " + cacheKey.getS3Prefix());
        }
    }

    /**
     * Checks only the immediate parent of the requested key for a cached exception.
     *
     * @param cacheKey the request cache key to check
     * @return the cached exception if the immediate parent has a negative entry, or null if not
     */
    public LakeFormationException getIfParentCached(final CacheKey cacheKey) {
        final Optional<String> parent = immediateParent(cacheKey.getS3Prefix());
        if (parent.isPresent()) {
            final LakeFormationException cached =
                exceptionCache.getIfPresent(negativeKey(cacheKey, parent.get()));
            if (cached != null) {
                LOGGER.info("Found cached negative response at parent: " + parent.get());
                return cached;
            }
        }
        return null;
    }

    /**
     * Builds a permission-agnostic negative-cache key for a given prefix.
     */
    private CacheKey negativeKey(final CacheKey cacheKey, final String prefix) {
        return new CacheKey(cacheKey.getCredentials(), NEGATIVE_CACHE_PERMISSION, prefix);
    }

    /**
     * Returns the immediate parent directory of an S3 path.
     *
     * <p>If the path ends with "/*" or "/", it is a directory reference and the normalized form
     * IS the parent directory (objects under it share this as their parent). Otherwise, the leaf
     * segment (file name) is dropped to find the parent.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code s3://bucket/f1/f2/file.csv} returns {@code s3://bucket/f1/f2}</li>
     *   <li>{@code s3://bucket/folder1/file.csv} returns {@code s3://bucket/folder1}</li>
     *   <li>{@code s3://bucket/file.csv} returns {@code s3://bucket}</li>
     *   <li>{@code s3://bucket/folder1/folder2/*} returns {@code s3://bucket/folder1/folder2}
     *       (directory reference)</li>
     *   <li>{@code s3://bucket/folder1/folder2/} returns {@code s3://bucket/folder1/folder2}
     *       (directory reference)</li>
     *   <li>{@code s3://bucket} returns empty (no parent above bucket root)</li>
     * </ul>
     *
     * @param s3Prefix the full S3 path
     * @return the immediate parent directory, or empty if no valid parent exists
     */
    private Optional<String> immediateParent(final String s3Prefix) {
        final String normalized = normalize(s3Prefix);
        if (!normalized.startsWith(S3_SCHEME)) {
            return Optional.empty();
        }

        // First "/" after the scheme separates the bucket from the key.
        final int bucketRootEnd = normalized.indexOf('/', S3_SCHEME.length());
        if (bucketRootEnd == -1) {
            // If original was a directory reference at bucket root (e.g., "s3://bucket/*"),
            // the normalized bucket root IS the parent for objects under it.
            if (s3Prefix.endsWith("/*") || s3Prefix.endsWith("/")) {
                return Optional.of(normalized);
            }
            // Only a bucket root was supplied; there is nothing above it to cache.
            return Optional.empty();
        }

        // If the original path ended with /* or /, it is a directory reference.
        // The normalized form IS the parent directory for objects under it.
        if (s3Prefix.endsWith("/*") || s3Prefix.endsWith("/")) {
            return Optional.of(normalized);
        }

        // Drop the leaf segment to get the immediate parent.
        final int lastSlash = normalized.lastIndexOf('/');
        if (lastSlash > bucketRootEnd) {
            return Optional.of(normalized.substring(0, lastSlash));
        }
        // The path is directly under the bucket root (e.g., s3://bucket/file.csv).
        final String bucketRoot = normalized.substring(0, bucketRootEnd);
        return Optional.of(bucketRoot);
    }

    /**
     * Strips a trailing "/*" or "/" from a prefix so directory / wildcard requests produce the
     * same keys as object requests under the same location. Mirrors the normalization done for
     * the positive cache when processing a matched grant target.
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
