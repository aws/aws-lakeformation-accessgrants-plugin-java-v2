package software.amazon.lakeformation.plugin.accessgrants.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for AccessGrantsCache.
 */
@ExtendWith(MockitoExtension.class)
public class AccessGrantsCacheTest {

    private AccessGrantsCache cache;
    private CacheKey testKey;
    private AwsCredentials testCredentials;

    @Mock
    private LakeFormationClient mockLakeFormationClient;

    @Mock
    private AccessDeniedCache mockAccessDeniedCache;

    @BeforeEach
    public void setUp() {
        cache = new AccessGrantsCache();
        testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");
        testKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/test-key");
    }

    @Test
    public void testCacheInitialization() {
        assertNotNull(cache);
    }

    @Test
    public void testCacheWithInvalidSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AccessGrantsCache(2000000, 3600); // Exceeds max size
        });
    }

    @Test
    public void testCacheWithInvalidDuration() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AccessGrantsCache(1000, 50000); // Exceeds max duration
        });
    }

    @Test
    public void testGetCredentialsFromLakeFormation() {
        // Mock successful LakeFormation response
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("tempAccessKey")
            .secretAccessKey("tempSecretKey")
            .sessionToken("tempSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of("s3://test-bucket/test-key"))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);

        assertNotNull(result);
        verify(mockLakeFormationClient).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertTrue(result instanceof AwsSessionCredentials);
        assertEquals("tempAccessKey", result.accessKeyId());
        assertEquals("tempSecretKey", result.secretAccessKey());
        assertEquals("tempSessionToken", ((AwsSessionCredentials) result).sessionToken());
    }

    @Test
    public void testGetCredentialsHandlesAccessDenied() {
        // Mock AccessDenied exception
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("AccessDenied")
            .errorMessage("Access Denied")
            .build();
        LakeFormationException accessDeniedException = (LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .message("Access Denied")
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(accessDeniedException);

        assertThrows(LakeFormationException.class, () -> {
            cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        });

        // Verify the exception was cached
        verify(mockAccessDeniedCache).putValueInCache(eq(testKey), any(LakeFormationException.class));
    }

    @Test
    public void testCacheMissTriggersApiCall() {
        // Mock successful LakeFormation response
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("tempAccessKey")
            .secretAccessKey("tempSecretKey")
            .sessionToken("tempSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of("s3://test-bucket/test-key"))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // First call should trigger API call
        AwsCredentials result1 = cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        assertNotNull(result1);
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));

        // Second call with same key should use cache (no additional API call)
        AwsCredentials result2 = cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        assertNotNull(result2);
        // Still only 1 API call since credentials should be cached
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Test
    public void testNonAccessDeniedExceptionNotCached() {
        // Mock a different LakeFormation exception (not AccessDenied)
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("InternalError")
            .errorMessage("Internal Server Error")
            .build();
        LakeFormationException internalException = (LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .message("Internal Server Error")
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(internalException);

        assertThrows(LakeFormationException.class, () -> {
            cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        });

        // Verify the exception was NOT cached (only AccessDenied should be cached)
        verify(mockAccessDeniedCache, never()).putValueInCache(any(), any());
    }

    @Test
    @DisplayName("Prefix-level search is attempted first before API call")
    public void testPrefixLevelSearchAttemptedFirst() {
        // First, populate cache with a parent prefix
        String parentPrefix = "s3://test-bucket/parent";
        CacheKey parentKey = new CacheKey(testCredentials, Permission.READ, parentPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("parentAccessKey")
            .secretAccessKey("parentSecretKey")
            .sessionToken("parentSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(parentPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials for parent prefix
        cache.getCredentials(mockLakeFormationClient, parentKey, mockAccessDeniedCache);
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));

        // Now request credentials for a child prefix - should hit cache
        CacheKey childKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/parent/child/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, childKey, mockAccessDeniedCache);

        // Should still be only 1 API call (cache hit for parent prefix)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("parentAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("Cache hit at bucket level prefix")
    public void testCacheHitAtBucketLevel() {
        String bucketPrefix = "s3://test-bucket";
        CacheKey bucketKey = new CacheKey(testCredentials, Permission.READ, bucketPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("bucketAccessKey")
            .secretAccessKey("bucketSecretKey")
            .sessionToken("bucketSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(bucketPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials for bucket level
        cache.getCredentials(mockLakeFormationClient, bucketKey, mockAccessDeniedCache);

        // Request credentials for a deeply nested path
        CacheKey deepKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/a/b/c/d/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, deepKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit at bucket level)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("bucketAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("Cache hit at intermediate prefix level")
    public void testCacheHitAtIntermediateLevel() {
        String intermediatePrefix = "s3://test-bucket/folder1/folder2";
        CacheKey intermediateKey = new CacheKey(testCredentials, Permission.READ, intermediatePrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("intermediateAccessKey")
            .secretAccessKey("intermediateSecretKey")
            .sessionToken("intermediateSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(intermediatePrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials for intermediate prefix
        cache.getCredentials(mockLakeFormationClient, intermediateKey, mockAccessDeniedCache);

        // Request credentials for a child path
        CacheKey childKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/folder1/folder2/folder3/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, childKey, mockAccessDeniedCache);

        // Should only have 1 API call
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("intermediateAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("No cache hit when prefix doesn't match - triggers API call")
    public void testNoCacheHitWhenPrefixDoesNotMatch() {
        String prefix1 = "s3://bucket-a/folder";
        CacheKey key1 = new CacheKey(testCredentials, Permission.READ, prefix1);

        TemporaryCredentials tempCreds1 = TemporaryCredentials.builder()
            .accessKeyId("accessKey1")
            .secretAccessKey("secretKey1")
            .sessionToken("sessionToken1")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse1 = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds1)
            .accessibleDataLocations(List.of(prefix1))
            .build();

        String prefix2 = "s3://bucket-b/folder";
        TemporaryCredentials tempCreds2 = TemporaryCredentials.builder()
            .accessKeyId("accessKey2")
            .secretAccessKey("secretKey2")
            .sessionToken("sessionToken2")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse2 = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds2)
            .accessibleDataLocations(List.of(prefix2))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse1)
            .thenReturn(mockResponse2);

        // Cache credentials for bucket-a
        cache.getCredentials(mockLakeFormationClient, key1, mockAccessDeniedCache);

        // Request credentials for bucket-b - should NOT hit cache
        CacheKey key2 = new CacheKey(testCredentials, Permission.READ, prefix2);
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, key2, mockAccessDeniedCache);

        // Should have 2 API calls (no cache hit)
        verify(mockLakeFormationClient, times(2)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("accessKey2", result.accessKeyId());
    }

    @Test
    @DisplayName("Character-level search with wildcard matching")
    public void testCharacterLevelSearchWithWildcard() {
        // Cache credentials with a wildcard pattern
        String wildcardPrefix = "s3://test-bucket/data*";
        CacheKey wildcardKey = new CacheKey(testCredentials, Permission.READ, wildcardPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("wildcardAccessKey")
            .secretAccessKey("wildcardSecretKey")
            .sessionToken("wildcardSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(wildcardPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with wildcard pattern
        cache.getCredentials(mockLakeFormationClient, wildcardKey, mockAccessDeniedCache);

        // Request credentials for a path that matches the wildcard
        CacheKey matchingKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/data-files/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, matchingKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit via character-level search)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("wildcardAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("Fallback from prefix to character level search")
    public void testFallbackFromPrefixToCharacterLevel() {
        // Cache credentials with a wildcard pattern (not a prefix match)
        String wildcardPrefix = "s3://test-bucket/folder/sub*";
        CacheKey wildcardKey = new CacheKey(testCredentials, Permission.READ, wildcardPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("wildcardAccessKey")
            .secretAccessKey("wildcardSecretKey")
            .sessionToken("wildcardSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(wildcardPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with wildcard pattern
        cache.getCredentials(mockLakeFormationClient, wildcardKey, mockAccessDeniedCache);

        // Request credentials for a path that won't match prefix but will match character-level
        // The path "s3://test-bucket/folder/subfolder/file.txt" should match "s3://test-bucket/folder/sub*"
        CacheKey requestKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/folder/subfolder/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, requestKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit via character-level search after prefix miss)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("wildcardAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("Character-level search at different depths")
    public void testCharacterLevelSearchAtDifferentDepths() {
        // Cache credentials with a wildcard at a specific depth
        String wildcardPrefix = "s3://test-bucket/level1/level2/dat*";
        CacheKey wildcardKey = new CacheKey(testCredentials, Permission.READ, wildcardPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("deepWildcardAccessKey")
            .secretAccessKey("deepWildcardSecretKey")
            .sessionToken("deepWildcardSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(wildcardPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with wildcard pattern
        cache.getCredentials(mockLakeFormationClient, wildcardKey, mockAccessDeniedCache);

        // Request credentials for a path that matches the wildcard at depth
        CacheKey matchingKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/level1/level2/data/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, matchingKey, mockAccessDeniedCache);

        // Should only have 1 API call
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("deepWildcardAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("READ request finds READWRITE cache entry")
    public void testReadRequestFindsReadWriteCacheEntry() {
        // Cache credentials with READWRITE permission
        String prefix = "s3://test-bucket/data";
        CacheKey readWriteKey = new CacheKey(testCredentials, Permission.READWRITE, prefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("readWriteAccessKey")
            .secretAccessKey("readWriteSecretKey")
            .sessionToken("readWriteSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(prefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with READWRITE permission
        cache.getCredentials(mockLakeFormationClient, readWriteKey, mockAccessDeniedCache);

        // Request credentials with READ permission - should find READWRITE entry
        CacheKey readKey = new CacheKey(testCredentials, Permission.READ, prefix);
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, readKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit via permission fallback)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("readWriteAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("WRITE request finds READWRITE cache entry")
    public void testWriteRequestFindsReadWriteCacheEntry() {
        // Cache credentials with READWRITE permission
        String prefix = "s3://test-bucket/data";
        CacheKey readWriteKey = new CacheKey(testCredentials, Permission.READWRITE, prefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("readWriteAccessKey")
            .secretAccessKey("readWriteSecretKey")
            .sessionToken("readWriteSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(prefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with READWRITE permission
        cache.getCredentials(mockLakeFormationClient, readWriteKey, mockAccessDeniedCache);

        // Request credentials with WRITE permission - should find READWRITE entry
        CacheKey writeKey = new CacheKey(testCredentials, Permission.WRITE, prefix);
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, writeKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit via permission fallback)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("readWriteAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("READ request finds READWRITE cache entry at parent prefix")
    public void testReadRequestFindsReadWriteAtParentPrefix() {
        // Cache credentials with READWRITE permission at parent prefix
        String parentPrefix = "s3://test-bucket/parent";
        CacheKey readWriteKey = new CacheKey(testCredentials, Permission.READWRITE, parentPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("parentReadWriteAccessKey")
            .secretAccessKey("parentReadWriteSecretKey")
            .sessionToken("parentReadWriteSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(parentPrefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // Cache credentials with READWRITE permission
        cache.getCredentials(mockLakeFormationClient, readWriteKey, mockAccessDeniedCache);

        // Request credentials with READ permission for child path
        CacheKey readKey = new CacheKey(testCredentials, Permission.READ, "s3://test-bucket/parent/child/file.txt");
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, readKey, mockAccessDeniedCache);

        // Should only have 1 API call (cache hit via permission fallback at parent prefix)
        verify(mockLakeFormationClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("parentReadWriteAccessKey", result.accessKeyId());
    }

    @Test
    @DisplayName("READWRITE request does not fallback to READ or WRITE")
    public void testReadWriteRequestDoesNotFallback() {
        // Cache credentials with READ permission only
        String prefix = "s3://test-bucket/data";
        CacheKey readKey = new CacheKey(testCredentials, Permission.READ, prefix);

        TemporaryCredentials tempCredsRead = TemporaryCredentials.builder()
            .accessKeyId("readAccessKey")
            .secretAccessKey("readSecretKey")
            .sessionToken("readSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponseRead = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCredsRead)
            .accessibleDataLocations(List.of(prefix))
            .build();

        TemporaryCredentials tempCredsReadWrite = TemporaryCredentials.builder()
            .accessKeyId("readWriteAccessKey")
            .secretAccessKey("readWriteSecretKey")
            .sessionToken("readWriteSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponseReadWrite = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCredsReadWrite)
            .accessibleDataLocations(List.of(prefix))
            .build();

        when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponseRead)
            .thenReturn(mockResponseReadWrite);

        // Cache credentials with READ permission
        cache.getCredentials(mockLakeFormationClient, readKey, mockAccessDeniedCache);

        // Request credentials with READWRITE permission - should NOT find READ entry
        CacheKey readWriteKey = new CacheKey(testCredentials, Permission.READWRITE, prefix);
        AwsCredentials result = cache.getCredentials(mockLakeFormationClient, readWriteKey, mockAccessDeniedCache);

        // Should have 2 API calls (no fallback from READWRITE to READ)
        verify(mockLakeFormationClient, times(2)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
        assertNotNull(result);
        assertEquals("readWriteAccessKey", result.accessKeyId());
    }
}
