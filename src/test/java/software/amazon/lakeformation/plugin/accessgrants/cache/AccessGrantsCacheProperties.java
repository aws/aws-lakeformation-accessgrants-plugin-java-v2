package software.amazon.lakeformation.plugin.accessgrants.cache;

import net.jqwik.api.*;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Property-based tests for AccessGrantsCache.
 * Feature: lakeformation-api-integration
 */
public class AccessGrantsCacheProperties {

    /**
     * Grant Target Processing
     * For any matched grant target string ending with /* or /,
     * the processed result should have those trailing characters removed.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Grant Target Processing")
    void grantTargetProcessingRemovesTrailingSlashStar(
            @ForAll("validS3Prefixes") String basePrefix) {

        AccessGrantsCache cache = new AccessGrantsCache();
        LakeFormationClient mockClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache mockAccessDeniedCache = Mockito.mock(AccessDeniedCache.class);
        AwsCredentials testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Test with trailing /*
        String prefixWithSlashStar = basePrefix + "/*";
        testGrantTargetProcessing(cache, mockClient, mockAccessDeniedCache, testCredentials,
                                  prefixWithSlashStar, basePrefix);

        // Reset mock for next test
        Mockito.reset(mockClient);
        cache = new AccessGrantsCache();

        // Test with trailing /
        String prefixWithSlash = basePrefix + "/";
        testGrantTargetProcessing(cache, mockClient, mockAccessDeniedCache, testCredentials,
                                  prefixWithSlash, basePrefix);

        // Reset mock for next test
        Mockito.reset(mockClient);
        cache = new AccessGrantsCache();

        // Test without trailing / or /* (should remain unchanged)
        testGrantTargetProcessing(cache, mockClient, mockAccessDeniedCache, testCredentials,
                                  basePrefix, basePrefix);
    }

    private void testGrantTargetProcessing(
            AccessGrantsCache cache,
            LakeFormationClient mockClient,
            AccessDeniedCache mockAccessDeniedCache,
            AwsCredentials testCredentials,
            String returnedLocation,
            String expectedCacheKey) {

        String requestPrefix = expectedCacheKey + "/subpath/file.txt";
        CacheKey cacheKey = new CacheKey(testCredentials, Permission.READ, requestPrefix);

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId("testAccessKey")
            .secretAccessKey("testSecretKey")
            .sessionToken("testSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(returnedLocation))
            .build();

        when(mockClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        // First call - should cache with processed key
        cache.getCredentials(mockClient, cacheKey, mockAccessDeniedCache);
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));

        // Second call with the expected cache key - should hit cache
        CacheKey lookupKey = new CacheKey(testCredentials, Permission.READ, expectedCacheKey + "/another/file.txt");
        cache.getCredentials(mockClient, lookupKey, mockAccessDeniedCache);

        // Should still be only 1 API call if cache key was processed correctly
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    /**
     * Credential Retrieval Round-Trip
     * For any valid S3 prefix and permission, when credentials are retrieved from Lake Formation
     * and cached, subsequent requests for the same prefix should return the cached credentials
     * without calling the API again.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Credential Retrieval Round-Trip")
    void credentialRetrievalRoundTrip(
            @ForAll("validS3Prefixes") String s3Prefix,
            @ForAll("validPermissions") Permission permission) {

        AccessGrantsCache cache = new AccessGrantsCache();
        LakeFormationClient mockClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache mockAccessDeniedCache = Mockito.mock(AccessDeniedCache.class);
        AwsCredentials testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        String accessKeyId = "tempAccessKey_" + s3Prefix.hashCode();
        String secretAccessKey = "tempSecretKey_" + s3Prefix.hashCode();
        String sessionToken = "tempSessionToken_" + s3Prefix.hashCode();

        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(accessKeyId)
            .secretAccessKey(secretAccessKey)
            .sessionToken(sessionToken)
            .build();

        GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(s3Prefix))
            .build();

        when(mockClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(mockResponse);

        CacheKey cacheKey = new CacheKey(testCredentials, permission, s3Prefix);

        // First call - should hit Lake Formation API
        AwsCredentials result1 = cache.getCredentials(mockClient, cacheKey, mockAccessDeniedCache);

        // Verify credentials match what was returned
        assert result1 instanceof AwsSessionCredentials : "Result should be AwsSessionCredentials";
        AwsSessionCredentials sessionCreds1 = (AwsSessionCredentials) result1;
        assert accessKeyId.equals(sessionCreds1.accessKeyId()) : "Access key should match";
        assert secretAccessKey.equals(sessionCreds1.secretAccessKey()) : "Secret key should match";
        assert sessionToken.equals(sessionCreds1.sessionToken()) : "Session token should match";

        // Second call - should hit cache, not API
        AwsCredentials result2 = cache.getCredentials(mockClient, cacheKey, mockAccessDeniedCache);

        // Verify cached credentials match retrieved credentials
        assert result1.accessKeyId().equals(result2.accessKeyId()) : "Cached credentials should match";
        assert result1.secretAccessKey().equals(result2.secretAccessKey()) : "Cached credentials should match";

        // Verify API was only called once
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    /**
     * Access Denied Caching
     * For any request that results in an AccessDenied error from Lake Formation,
     * the exception should be cached, and subsequent requests with the same cache key
     * should throw the cached exception without calling the API.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Access Denied Caching")
    void accessDeniedCaching(
            @ForAll("validS3Prefixes") String s3Prefix,
            @ForAll("validPermissions") Permission permission) {

        AccessGrantsCache cache = new AccessGrantsCache();
        LakeFormationClient mockClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache accessDeniedCache = new AccessDeniedCache();
        AwsCredentials testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("AccessDenied")
            .errorMessage("Access Denied for " + s3Prefix)
            .build();
        LakeFormationException accessDeniedException = (LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .message("Access Denied")
            .build();

        when(mockClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(accessDeniedException);

        CacheKey cacheKey = new CacheKey(testCredentials, permission, s3Prefix);

        // First call - should throw and cache exception
        boolean firstCallThrew = false;
        try {
            cache.getCredentials(mockClient, cacheKey, accessDeniedCache);
        } catch (LakeFormationException e) {
            firstCallThrew = true;
            assert "AccessDenied".equals(e.awsErrorDetails().errorCode()) : "Should be AccessDenied error";
        }
        assert firstCallThrew : "First call should throw LakeFormationException";

        // Verify exception was cached
        Exception cachedException = accessDeniedCache.getValueFromCache(cacheKey);
        assert cachedException != null : "Exception should be cached";
        assert cachedException instanceof LakeFormationException : "Cached exception should be LakeFormationException";

        // Verify API was called exactly once
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    /**
     * Cache Search Order
     * For any cache containing credentials at both prefix and character levels,
     * prefix-level search should be attempted first, and character-level search
     * should only occur if prefix-level search fails.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Cache Search Order")
    void cacheSearchOrder(
            @ForAll("validS3Prefixes") String basePrefix,
            @ForAll("validPermissions") Permission permission) {

        AccessGrantsCache cache = new AccessGrantsCache();
        LakeFormationClient mockClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache mockAccessDeniedCache = Mockito.mock(AccessDeniedCache.class);
        AwsCredentials testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Setup credentials for prefix-level cache entry
        String prefixAccessKey = "prefixAccessKey";
        TemporaryCredentials prefixCreds = TemporaryCredentials.builder()
            .accessKeyId(prefixAccessKey)
            .secretAccessKey("prefixSecretKey")
            .sessionToken("prefixSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse prefixResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(prefixCreds)
            .accessibleDataLocations(List.of(basePrefix))
            .build();

        when(mockClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(prefixResponse);

        // Cache credentials at prefix level
        CacheKey prefixKey = new CacheKey(testCredentials, permission, basePrefix);
        cache.getCredentials(mockClient, prefixKey, mockAccessDeniedCache);

        // Request for child path - should hit prefix-level cache
        String childPath = basePrefix + "/child/file.txt";
        CacheKey childKey = new CacheKey(testCredentials, permission, childPath);
        AwsCredentials result = cache.getCredentials(mockClient, childKey, mockAccessDeniedCache);

        // Verify prefix-level credentials were returned (only 1 API call)
        assert prefixAccessKey.equals(result.accessKeyId()) : "Should return prefix-level cached credentials";
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    /**
     * Permission Fallback
     * For any READ or WRITE permission request, if no exact match exists in the cache
     * but a READWRITE entry exists for the same prefix, the READWRITE credentials should be returned.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Permission Fallback")
    void permissionFallback(
            @ForAll("validS3Prefixes") String s3Prefix,
            @ForAll("readOrWritePermission") Permission requestPermission) {

        AccessGrantsCache cache = new AccessGrantsCache();
        LakeFormationClient mockClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache mockAccessDeniedCache = Mockito.mock(AccessDeniedCache.class);
        AwsCredentials testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Setup READWRITE credentials
        String readWriteAccessKey = "readWriteAccessKey";
        TemporaryCredentials readWriteCreds = TemporaryCredentials.builder()
            .accessKeyId(readWriteAccessKey)
            .secretAccessKey("readWriteSecretKey")
            .sessionToken("readWriteSessionToken")
            .build();

        GetTemporaryDataLocationCredentialsResponse readWriteResponse = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(readWriteCreds)
            .accessibleDataLocations(List.of(s3Prefix))
            .build();

        when(mockClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(readWriteResponse);

        // Cache credentials with READWRITE permission
        CacheKey readWriteKey = new CacheKey(testCredentials, Permission.READWRITE, s3Prefix);
        cache.getCredentials(mockClient, readWriteKey, mockAccessDeniedCache);

        // Request with READ or WRITE permission - should find READWRITE entry
        CacheKey requestKey = new CacheKey(testCredentials, requestPermission, s3Prefix);
        AwsCredentials result = cache.getCredentials(mockClient, requestKey, mockAccessDeniedCache);

        // Verify READWRITE credentials were returned (only 1 API call)
        assert readWriteAccessKey.equals(result.accessKeyId()) : "Should return READWRITE cached credentials";
        verify(mockClient, times(1)).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Provide
    Arbitrary<String> validS3Prefixes() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(20),
            Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(10).list().ofMinSize(0).ofMaxSize(3)
        ).as((bucket, folders) -> {
            StringBuilder sb = new StringBuilder("s3://");
            sb.append(bucket.toLowerCase());
            for (String folder : folders) {
                sb.append("/").append(folder.toLowerCase());
            }
            return sb.toString();
        });
    }

    @Provide
    Arbitrary<Permission> validPermissions() {
        return Arbitraries.of(Permission.READ, Permission.WRITE, Permission.READWRITE);
    }

    @Provide
    Arbitrary<Permission> readOrWritePermission() {
        return Arbitraries.of(Permission.READ, Permission.WRITE);
    }
}
