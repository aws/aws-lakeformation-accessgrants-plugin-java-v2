package software.amazon.lakeformation.plugin.accessgrants.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

/**
 * Test class for ExceptionCache verifying immediate-parent-only caching behavior.
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
     * Builds a cache key under {@link Permission#READWRITE} - the single sentinel permission
     * that every ExceptionCache entry is stored and looked up under, regardless of the
     * request's permission.
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
        assertTrue(ExceptionCache.isNegativeCacheable(
            EntityNotFoundException.builder().build()));
    }

    @Test
    public void testIsNotNegativeCacheableForOtherExceptions() {
        assertFalse(ExceptionCache.isNegativeCacheable(ThrottledException.builder().build()));
        assertFalse(ExceptionCache.isNegativeCacheable(
            InvalidInputException.builder().build()));
        assertFalse(ExceptionCache.isNegativeCacheable(
            (LakeFormationException) LakeFormationException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode("AccessDenied").build())
                .build()));
    }

    @Test
    public void testMissWhenEmpty() {
        assertNull(cache.getIfParentCached(key("s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testExactPrefixHit() {
        // Cache the failure for a deeply-nested object.
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/folder2/folder3/file.csv"), conflictException);

        // A repeat request for the same object hits via its immediate parent (folder3).
        assertSame(conflictException,
            cache.getIfParentCached(
                key("s3://bucket/folder1/folder2/folder3/file.csv")));
    }

    @Test
    public void testSiblingObjectUnderSameFolderHits() {
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/folder2/folder3/fileA.csv"), conflictException);

        // A different object under the same folder shares the immediate parent -> hit.
        assertSame(conflictException,
            cache.getIfParentCached(
                key("s3://bucket/folder1/folder2/folder3/fileB.csv")));
    }

    @Test
    public void testSiblingFolderUnderSharedAncestorMisses() {
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/folder2/folder3/file.csv"), conflictException);

        // An object under a sibling sub-folder has a DIFFERENT immediate parent -> MISS.
        // Only the immediate parent (folder3) was cached, not folder2 or folder1.
        assertNull(cache.getIfParentCached(
            key("s3://bucket/folder1/folder2/otherFolder/file.csv")));
    }

    @Test
    public void testBucketRootIsNotCachedSoUnrelatedPathMisses() {
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/deep/file.csv"), conflictException);

        // A completely unrelated key in the SAME bucket MISSES, since bucket root
        // is NOT cached (only the immediate parent "deep" was cached).
        assertNull(cache.getIfParentCached(
            key("s3://bucket/unrelated/path/other.csv")));
    }

    @Test
    public void testDifferentBucketMisses() {
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/file.csv"), conflictException);

        // A different bucket must never be suppressed.
        assertNull(cache.getIfParentCached(
            key("s3://other-bucket/folder1/file.csv")));
    }

    @Test
    public void testPermissionAgnosticAcrossReadWrite() {
        // Failure recorded for one permission must be found regardless of the
        // requesting permission.
        cache.cacheForImmediateParent(
            key(Permission.WRITE, "s3://bucket/folder1/file.csv"), conflictException);

        assertSame(conflictException,
            cache.getIfParentCached(
                key(Permission.READ, "s3://bucket/folder1/file.csv")));
        assertSame(conflictException,
            cache.getIfParentCached(
                key(Permission.READWRITE, "s3://bucket/folder1/file.csv")));
        assertSame(conflictException,
            cache.getIfParentCached(
                key(Permission.WRITE, "s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testDifferentCredentialsMiss() {
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/file.csv"), conflictException);

        // A different principal is isolated by the cache key.
        AwsCredentialsIdentity otherCredentials =
            AwsBasicCredentials.create("otherKey", "otherSecret");
        assertNull(cache.getIfParentCached(
            new CacheKey(otherCredentials, Permission.READWRITE,
                "s3://bucket/folder1/file.csv")));
    }

    @Test
    public void testTrailingSlashAndWildcardNormalizeConsistently() {
        // A directory / wildcard request must produce the same keys as an object
        // request under it.
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/folder2/*"), conflictException);

        assertSame(conflictException,
            cache.getIfParentCached(key("s3://bucket/folder1/folder2/file.csv")));

        ExceptionCache slashCache = new ExceptionCache();
        slashCache.cacheForImmediateParent(
            key("s3://bucket/folder1/folder2/"), conflictException);
        assertSame(conflictException,
            slashCache.getIfParentCached(
                key("s3://bucket/folder1/folder2/file.csv")));
    }

    @Test
    public void testEntityNotFoundIsStoredAndRetrieved() {
        EntityNotFoundException entityNotFound = EntityNotFoundException.builder()
            .awsErrorDetails(
                AwsErrorDetails.builder().errorCode("EntityNotFoundException").build())
            .message("A specified entity does not exist")
            .build();
        cache.cacheForImmediateParent(
            key("s3://bucket/folder1/file.csv"), entityNotFound);

        // Sibling in same folder should hit (they share immediate parent "folder1").
        assertSame(entityNotFound,
            cache.getIfParentCached(key("s3://bucket/folder1/other.csv")));
    }

    @Test
    public void testDifferentSubfolderMisses() {
        // A path in a different subfolder should NOT be poisoned.
        cache.cacheForImmediateParent(
            key("s3://bucket/warehouse/table_a/_delta_log/file.json"),
            conflictException);

        // table_b is a sibling of table_a but under a different immediate parent.
        assertNull(cache.getIfParentCached(
            key("s3://bucket/warehouse/table_b/_delta_log/file.json")));
    }

    @Test
    public void testUnrelatedPathInSameBucketMisses() {
        // A completely different path in the same bucket should NOT be poisoned.
        cache.cacheForImmediateParent(
            key("s3://my-bucket/data/sales/2024/report.parquet"), conflictException);

        // Completely unrelated path in same bucket.
        assertNull(cache.getIfParentCached(
            key("s3://my-bucket/logs/access/2024/log.csv")));
        // Even a sibling year under sales has different immediate parent.
        assertNull(cache.getIfParentCached(
            key("s3://my-bucket/data/sales/2025/report.parquet")));
    }

    @Test
    public void testBucketRootOnlyPathCachesNothing() {
        // A bucket-root-only path has no parent to cache at.
        cache.cacheForImmediateParent(key("s3://bucket"), conflictException);

        // Nothing should be cached since there is no parent for a bare bucket root.
        assertNull(cache.getIfParentCached(key("s3://bucket/anything.csv")));
    }

    @Test
    public void testBucketRootWildcardCachesAtBucketRoot() {
        // A bucket-root wildcard (s3://bucket/*) is a directory reference at the bucket level.
        // It should cache at the bucket root so that files directly under the bucket are suppressed.
        cache.cacheForImmediateParent(key("s3://bucket/*"), conflictException);

        assertSame(conflictException,
            cache.getIfParentCached(key("s3://bucket/file.csv")));
    }

    @Test
    public void testBucketRootTrailingSlashCachesAtBucketRoot() {
        // A bucket-root trailing slash (s3://bucket/) is a directory reference at the bucket level.
        cache.cacheForImmediateParent(key("s3://bucket/"), conflictException);

        assertSame(conflictException,
            cache.getIfParentCached(key("s3://bucket/file.csv")));
    }
}
