package software.amazon.lakeformation.plugin.accessgrants.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.lakeformation.model.ConflictException;
import software.amazon.awssdk.services.lakeformation.model.EntityNotFoundException;
import software.amazon.awssdk.services.lakeformation.model.InvalidInputException;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.lakeformation.model.ThrottledException;
import software.amazon.awssdk.services.s3control.model.Permission;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ExceptionCache.
 */
public class ExceptionCacheTest {

    private ExceptionCache cache;
    private AwsCredentialsIdentity testCredentials;
    private ConflictException conflictException;

    @BeforeEach
    public void setUp() {
        cache = new ExceptionCache();
        testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");
        conflictException = ConflictException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ConflictException").build())
            .message("Multiple tables registered for the same S3 location")
            .build();
    }

    private CacheKey key(final Permission permission, final String s3Prefix) {
        return new CacheKey(testCredentials, permission, s3Prefix);
    }

    /**
     * Builds a cache key under {@link Permission#READWRITE} - the single sentinel permission that every
     * ExceptionCache entry is stored and looked up under, regardless of the request's permission. Tests
     * that are not specifically about permission use this so the key mirrors what actually lands in the
     * cache; cross-permission behavior is covered by testPermissionAgnosticAcrossReadWrite.
     */
    private CacheKey key(final String s3Prefix) {
        return new CacheKey(testCredentials, Permission.READWRITE, s3Prefix);
    }

    @Test
    public void testCacheInitialization() {
        assertNotNull(new ExceptionCache());
        assertNotNull(new ExceptionCache(1000, 60));
    }

    @Test
    public void testIsNegativeCacheableForConflictAndEntityNotFound() {
        assertTrue(ExceptionCache.isNegativeCacheable(ConflictException.builder().build()));
        assertTrue(ExceptionCache.isNegativeCacheable(EntityNotFoundException.builder().build()));
    }

    @Test
    public void testIsNotNegativeCacheableForOtherExceptions() {
        // Retryable and other errors must NOT be negative-cached;
        assertFalse(ExceptionCache.isNegativeCacheable(ThrottledException.builder().build()));
        assertFalse(ExceptionCache.isNegativeCacheable(InvalidInputException.builder().build()));
        assertFalse(ExceptionCache.isNegativeCacheable((LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
            .build()));
    }

    @Test
    public void testMissWhenEmpty() {
        assertNull(cache.getIfPrefixCached(key("s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testExactPrefixHit() {
        // Cache the failure for a deeply-nested object.
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/folder2/folder3/file.csv"), conflictException);

        // A repeat request for the same object is short-circuited via its parent prefixes.
        assertSame(conflictException,
            cache.getIfPrefixCached(key("s3://bucket/folder1/folder2/folder3/file.csv")));
    }

    @Test
    public void testSiblingObjectUnderSameFolderHits() {
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/folder2/folder3/fileA.csv"), conflictException);

        // A different object under the same folder shares the folder3 prefix -> hit.
        assertSame(conflictException,
            cache.getIfPrefixCached(key("s3://bucket/folder1/folder2/folder3/fileB.csv")));
    }

    @Test
    public void testSiblingFolderUnderSharedAncestorHits() {
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/folder2/folder3/file.csv"), conflictException);

        // An object under a sibling sub-folder shares the folder1/folder2 ancestor -> hit, because
        // entries are stored all the way up to the bucket root.
        assertSame(conflictException,
            cache.getIfPrefixCached(key("s3://bucket/folder1/folder2/otherFolder/file.csv")));
    }

    @Test
    public void testBucketRootIsCachedSoAnyObjectInBucketHits() {
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/deep/file.csv"), conflictException);

        // A completely unrelated key in the SAME bucket still hits, since the bucket root is cached.
        assertSame(conflictException,
            cache.getIfPrefixCached(key("s3://bucket/unrelated/path/other.csv")));
    }

    @Test
    public void testDifferentBucketMisses() {
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/file.csv"), conflictException);

        // The walk stops at the bucket root, so a different bucket must never be suppressed.
        assertNull(cache.getIfPrefixCached(key("s3://other-bucket/folder1/file.csv")));
    }

    @Test
    public void testPermissionAgnosticAcrossReadWrite() {
        // Failure recorded for one permission must be found regardless of the requesting permission.
        cache.cacheForAllPrefixes(
            key(Permission.WRITE, "s3://bucket/folder1/file.csv"), conflictException);

        assertSame(conflictException,
            cache.getIfPrefixCached(key(Permission.READ, "s3://bucket/folder1/file.csv")));
        assertSame(conflictException,
            cache.getIfPrefixCached(key(Permission.READWRITE, "s3://bucket/folder1/file.csv")));
        assertSame(conflictException,
            cache.getIfPrefixCached(key(Permission.WRITE, "s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testDifferentCredentialsMiss() {
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/file.csv"), conflictException);

        // A different principal is isolated by the cache key.
        AwsCredentialsIdentity otherCredentials = AwsBasicCredentials.create("otherKey", "otherSecret");
        assertNull(cache.getIfPrefixCached(
            new CacheKey(otherCredentials, Permission.READWRITE, "s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testTrailingSlashAndWildcardNormalizeConsistently() {
        // A directory / wildcard request must produce the same keys as an object request under it.
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/folder2/*"), conflictException);

        assertSame(conflictException,
            cache.getIfPrefixCached(key("s3://bucket/folder1/folder2/file.csv")));

        ExceptionCache slashCache = new ExceptionCache();
        slashCache.cacheForAllPrefixes(key("s3://bucket/folder1/folder2/"), conflictException);
        assertSame(conflictException,
            slashCache.getIfPrefixCached(key("s3://bucket/folder1/folder2/file.csv")));
    }

    @Test
    public void testEntityNotFoundIsStoredAndRetrieved() {
        EntityNotFoundException entityNotFound = EntityNotFoundException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("EntityNotFoundException").build())
            .message("A specified entity does not exist")
            .build();
        cache.cacheForAllPrefixes(key("s3://bucket/folder1/file.csv"), entityNotFound);

        assertSame(entityNotFound,
            cache.getIfPrefixCached(key("s3://bucket/folder1/other.csv")));
    }
}
