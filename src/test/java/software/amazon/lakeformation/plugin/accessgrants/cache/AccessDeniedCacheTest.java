package software.amazon.lakeformation.plugin.accessgrants.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

/**
 * Test class for AccessDeniedCache.
 */
public class AccessDeniedCacheTest {

    private AccessDeniedCache cache;
    private CacheKey testKey;
    private Exception testException;

    @BeforeEach
    public void setUp() {
        cache = new AccessDeniedCache();
        testKey = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
                Permission.READ,
            "s3://test-bucket/test-key"
        );
        testException = new RuntimeException("Access denied");
    }

    @Test
    public void testPutAndGetValue() {
        cache.putValueInCache(testKey, testException);
        Exception retrieved = cache.getValueFromCache(testKey);

        assertEquals(testException, retrieved);
    }

    @Test
    public void testGetNonExistentValue() {
        Exception retrieved = cache.getValueFromCache(testKey);
        assertNull(retrieved);
    }

    @Test
    public void testCacheWithCustomSizeAndTtl() {
        AccessDeniedCache customCache = new AccessDeniedCache(1000, 300);
        assertNotNull(customCache);
    }

    @Test
    public void testParentLevelCaching() {
        // Store for file1.parquet, lookup for file2.parquet in the same folder -> HIT
        CacheKey file1Key = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder1/folder2/file1.parquet"
        );
        CacheKey file2Key = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder1/folder2/file2.parquet"
        );
        Exception exception = new RuntimeException("Access denied for file1");

        cache.putValueInCache(file1Key, exception);

        // file2 in the same folder should hit the parent-level cache entry
        Exception retrieved = cache.getValueFromCache(file2Key);
        assertNotNull(retrieved, "Sibling file should hit parent-level cache entry");
        assertEquals(exception, retrieved);
    }

    @Test
    public void testDifferentFolderMisses() {
        // Store for folder1/file.parquet, lookup for folder2/file.parquet -> MISS
        CacheKey folder1FileKey = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder1/file.parquet"
        );
        CacheKey folder2FileKey = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder2/file.parquet"
        );
        Exception exception = new RuntimeException("Access denied");

        cache.putValueInCache(folder1FileKey, exception);

        // file in a different folder should NOT match
        Exception retrieved = cache.getValueFromCache(folder2FileKey);
        assertNull(retrieved, "File in a different folder should not match");
    }

    @Test
    public void testExactKeyStillWorks() {
        // Store for exact path, lookup same path -> HIT (existing behavior preserved)
        Exception exception = new RuntimeException("Access denied exact");

        cache.putValueInCache(testKey, exception);

        Exception retrieved = cache.getValueFromCache(testKey);
        assertNotNull(retrieved, "Exact key lookup should still work");
        assertEquals(exception, retrieved);
    }

    @Test
    public void testTrailingSlashHandled() {
        // Store for folder/, lookup for folder/file.parquet -> HIT
        CacheKey folderKey = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder1/folder2/"
        );
        CacheKey fileInFolderKey = new CacheKey(
            AwsBasicCredentials.create("accessKey", "secretKey"),
            Permission.READ,
            "s3://test-bucket/folder1/folder2/file.parquet"
        );
        Exception exception = new RuntimeException("Access denied for folder");

        cache.putValueInCache(folderKey, exception);

        // A file inside that folder should hit the parent-level entry
        Exception retrieved = cache.getValueFromCache(fileInFolderKey);
        assertNotNull(retrieved, "File in folder with trailing slash should hit cache");
        assertEquals(exception, retrieved);
    }
}
