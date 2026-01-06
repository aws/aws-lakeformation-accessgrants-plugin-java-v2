package software.amazon.lakeformation.plugin.accessgrants.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import static org.junit.jupiter.api.Assertions.*;

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
}
