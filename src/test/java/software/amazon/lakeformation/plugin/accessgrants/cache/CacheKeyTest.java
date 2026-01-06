package software.amazon.lakeformation.plugin.accessgrants.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for CacheKey.
 */
public class CacheKeyTest {

    private AwsCredentials testCredentials;
    private CacheKey cacheKey;

    @BeforeEach
    public void setUp() {
        testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");
        cacheKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/test-key");
    }

    @Test
    public void testCacheKeyCreation() {
        assertNotNull(cacheKey);
        assertEquals(testCredentials, cacheKey.getCredentials());
        assertEquals(Permission.READ, cacheKey.getPermission());
        assertEquals("s3://test-bucket/test-key", cacheKey.getS3Prefix());
    }

    @Test
    public void testCacheKeyEquality() {
        CacheKey sameKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/test-key");
        CacheKey differentKey = new CacheKey(testCredentials, Permission.WRITE, "s3://test-bucket/test-key");

        assertEquals(cacheKey, sameKey);
        assertNotEquals(cacheKey, differentKey);
        assertEquals(cacheKey.hashCode(), sameKey.hashCode());
    }

    @Test
    public void testCacheKeyWithNullValues() {
        assertThrows(IllegalArgumentException.class, () -> {
            new CacheKey((AwsCredentials) null, Permission.READ, "s3://test-bucket/test-key");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            new CacheKey(testCredentials, null, "s3://test-bucket/test-key");
        });

        assertThrows(IllegalArgumentException.class, () -> {
            new CacheKey(testCredentials, Permission.READ, null);
        });
    }
}
