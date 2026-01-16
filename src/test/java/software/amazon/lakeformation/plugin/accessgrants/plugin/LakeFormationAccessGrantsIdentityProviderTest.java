package software.amazon.lakeformation.plugin.accessgrants.plugin;

import software.amazon.lakeformation.plugin.accessgrants.cache.AccessDeniedCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.AccessGrantsCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

/**
 * Test class for LakeFormationAccessGrantsIdentityProvider.
 * Tests successful credential resolution, access denied cache hit,
 * fallback to S3 Access Grants, and failure without fallback.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LakeFormationAccessGrantsIdentityProviderTest {

    @Mock
    private IdentityProvider<? extends AwsCredentialsIdentity> mockOriginalProvider;

    @Mock
    private LakeFormationClient mockLfClient;

    private AccessDeniedCache accessDeniedCache;
    private AccessGrantsCache accessGrantsCache;

    @Mock
    private IdentityProvider<? extends AwsCredentialsIdentity> mockS3AccessGrantsIdentityProvider;

    @Mock
    private ResolveIdentityRequest mockResolveIdentityRequest;

    private LakeFormationAccessGrantsIdentityProvider identityProvider;
    private AwsCredentialsIdentity testCredentials;

    private static final String TEST_ACCESS_KEY = "lfAccessKey";
    private static final String TEST_SECRET_KEY = "lfSecretKey";
    private static final String TEST_SESSION_TOKEN = "lfSessionToken";
    private static final String TEST_S3_PREFIX = "s3://test-bucket/test-key";

    @BeforeEach
    public void setUp() {
        testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Use real cache instances
        accessDeniedCache = new AccessDeniedCache();
        accessGrantsCache = new AccessGrantsCache();

        identityProvider = new LakeFormationAccessGrantsIdentityProvider(
            mockOriginalProvider,
            mockLfClient,
            accessDeniedCache,
            accessGrantsCache,
            true, // enableFallback
            mockS3AccessGrantsIdentityProvider
        );

        // Setup common mock behaviors
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(mockResolveIdentityRequest);
        when(mockResolveIdentityRequest.property(PREFIX_PROPERTY))
            .thenReturn(TEST_S3_PREFIX);
        when(mockResolveIdentityRequest.property(PERMISSION_PROPERTY))
            .thenReturn(Permission.READ.toString());
    }

    @Test
    public void testIdentityType() {
        assertEquals(AwsCredentialsIdentity.class, identityProvider.identityType());
    }

    @Test
    public void testResolveIdentityWithSuccessfulLakeFormationCredentials() throws Exception {
        // Setup Lake Formation to return credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(TEST_S3_PREFIX))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        CompletableFuture<? extends AwsCredentialsIdentity> result =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);

        AwsCredentialsIdentity resolvedCredentials = result.get();
        assertNotNull(resolvedCredentials);
        assertEquals(TEST_ACCESS_KEY, resolvedCredentials.accessKeyId());
        assertEquals(TEST_SECRET_KEY, resolvedCredentials.secretAccessKey());
        assertTrue(resolvedCredentials instanceof AwsSessionCredentials);
        assertEquals(TEST_SESSION_TOKEN, ((AwsSessionCredentials) resolvedCredentials).sessionToken());
    }

    @Test
    public void testResolveIdentityWithCachedAccessDenied() throws ExecutionException, InterruptedException {
        // Setup Lake Formation to throw access denied
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("AccessDenied")
            .errorMessage("Access Denied")
            .build();
        LakeFormationException accessDeniedException = (LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .message("Access Denied")
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(accessDeniedException);

        // Mock S3AccessGrants fallback success
        AwsCredentialsIdentity fallbackCredentials = AwsBasicCredentials.create("fallbackKey", "fallbackSecret");
        doReturn(CompletableFuture.completedFuture(fallbackCredentials))
            .when(mockS3AccessGrantsIdentityProvider).resolveIdentity(mockResolveIdentityRequest);

        // First call - should hit Lake Formation, get denied, cache exception, fallback to S3AccessGrants
        CompletableFuture<? extends AwsCredentialsIdentity> result1 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity resolvedCredentials1 = result1.get();

        // Second call - should hit access denied cache and fallback
        CompletableFuture<? extends AwsCredentialsIdentity> result2 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity resolvedCredentials2 = result2.get();

        assertNotNull(resolvedCredentials1);
        assertNotNull(resolvedCredentials2);
        assertEquals("fallbackKey", resolvedCredentials1.accessKeyId());
        assertEquals("fallbackKey", resolvedCredentials2.accessKeyId());

        // Verify Lake Formation was only called once (second call hit cache)
        verify(mockLfClient, times(1))
            .getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Test
    public void testResolveIdentityFallsBackToS3AccessGrants() throws Exception {
        // Mock Lake Formation failure with non-access-denied error
        RuntimeException lfException = new RuntimeException("Lake Formation service error");
        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(lfException);

        // Mock S3AccessGrants fallback success
        AwsCredentialsIdentity fallbackCredentials = AwsBasicCredentials.create("fallbackKey", "fallbackSecret");
        doReturn(CompletableFuture.completedFuture(fallbackCredentials))
            .when(mockS3AccessGrantsIdentityProvider).resolveIdentity(mockResolveIdentityRequest);

        CompletableFuture<? extends AwsCredentialsIdentity> result =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);

        AwsCredentialsIdentity resolvedCredentials = result.get();
        assertNotNull(resolvedCredentials);
        assertEquals("fallbackKey", resolvedCredentials.accessKeyId());
        assertEquals("fallbackSecret", resolvedCredentials.secretAccessKey());

        verify(mockS3AccessGrantsIdentityProvider).resolveIdentity(mockResolveIdentityRequest);
    }

    @Test
    public void testResolveIdentityFailsWhenFallbackDisabled() {
        // Create provider with fallback disabled
        LakeFormationAccessGrantsIdentityProvider providerWithoutFallback =
            new LakeFormationAccessGrantsIdentityProvider(
                mockOriginalProvider,
                mockLfClient,
                accessDeniedCache,
                accessGrantsCache,
                false, // fallback disabled
                mockS3AccessGrantsIdentityProvider
            );

        // Mock Lake Formation failure
        RuntimeException lfException = new RuntimeException("Lake Formation service error");
        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(lfException);

        CompletableFuture<? extends AwsCredentialsIdentity> result =
            providerWithoutFallback.resolveIdentity(mockResolveIdentityRequest);

        // Should fail with SdkClientException
        assertTrue(result.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, result::get);
        assertTrue(ex.getCause() instanceof SdkClientException);
        assertTrue(ex.getCause().getMessage().contains("Failed to resolve Lake Formation credentials"));

        // Verify S3AccessGrants was NOT called
        verify(mockS3AccessGrantsIdentityProvider, never()).resolveIdentity(any(ResolveIdentityRequest.class));
    }

    @Test
    public void testResolveIdentityFailsWhenNoFallbackProvider() {
        // Create provider without S3AccessGrants fallback provider
        LakeFormationAccessGrantsIdentityProvider providerWithoutFallback =
            new LakeFormationAccessGrantsIdentityProvider(
                mockOriginalProvider,
                mockLfClient,
                accessDeniedCache,
                accessGrantsCache,
                true, // fallback enabled but no provider
                null  // no fallback provider
            );

        // Mock Lake Formation failure
        RuntimeException lfException = new RuntimeException("Lake Formation service error");
        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(lfException);

        CompletableFuture<? extends AwsCredentialsIdentity> result =
            providerWithoutFallback.resolveIdentity(mockResolveIdentityRequest);

        // Should fail with SdkClientException
        assertTrue(result.isCompletedExceptionally());
    }

    @Test
    public void testResolveIdentityReturnsAwsSessionCredentials() throws Exception {
        // Setup Lake Formation to return session credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(TEST_S3_PREFIX))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        CompletableFuture<? extends AwsCredentialsIdentity> result =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);

        AwsCredentialsIdentity resolvedCredentials = result.get();

        // Verify it's an AwsSessionCredentials instance (Requirement 3.3)
        assertTrue(resolvedCredentials instanceof AwsSessionCredentials,
            "Resolved credentials should be AwsSessionCredentials");

        AwsSessionCredentials sessionCreds = (AwsSessionCredentials) resolvedCredentials;
        assertEquals(TEST_ACCESS_KEY, sessionCreds.accessKeyId());
        assertEquals(TEST_SECRET_KEY, sessionCreds.secretAccessKey());
        assertEquals(TEST_SESSION_TOKEN, sessionCreds.sessionToken());
    }

    @Test
    public void testSuccessfulCredentialsCacheHit() throws ExecutionException, InterruptedException {
        // Setup Lake Formation to return credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(TEST_S3_PREFIX))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        // First call - should hit Lake Formation and cache result
        CompletableFuture<? extends AwsCredentialsIdentity> result1 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials1 = result1.get();

        // Second call - should hit cache, not Lake Formation
        CompletableFuture<? extends AwsCredentialsIdentity> result2 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials2 = result2.get();

        // Third call - should also hit cache
        CompletableFuture<? extends AwsCredentialsIdentity> result3 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials3 = result3.get();

        // Verify all calls return same cached credentials
        assertEquals(TEST_ACCESS_KEY, credentials1.accessKeyId());
        assertEquals(TEST_ACCESS_KEY, credentials2.accessKeyId());
        assertEquals(TEST_ACCESS_KEY, credentials3.accessKeyId());

        // Verify Lake Formation was only called once
        verify(mockLfClient, times(1))
            .getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Test
    public void testAccessDeniedCacheHitMultipleCalls() throws ExecutionException, InterruptedException {
        // Setup Lake Formation to throw access denied
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("AccessDenied")
            .errorMessage("Access Denied")
            .build();
        LakeFormationException accessDeniedException = (LakeFormationException) LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .message("Access Denied")
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(accessDeniedException);

        // Setup S3 Access Grants fallback
        AwsCredentialsIdentity fallbackCredentials = AwsBasicCredentials.create("fallbackKey", "fallbackSecret");
        doReturn(CompletableFuture.completedFuture(fallbackCredentials))
            .when(mockS3AccessGrantsIdentityProvider).resolveIdentity(mockResolveIdentityRequest);

        // First call - should hit Lake Formation, get denied, cache exception, fallback to S3AccessGrants
        CompletableFuture<? extends AwsCredentialsIdentity> result1 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials1 = result1.get();

        // Second call - should hit access denied cache, skip Lake Formation, go to fallback
        CompletableFuture<? extends AwsCredentialsIdentity> result2 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials2 = result2.get();

        // Third call - should also hit access denied cache
        CompletableFuture<? extends AwsCredentialsIdentity> result3 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        AwsCredentialsIdentity credentials3 = result3.get();

        // Verify all calls return fallback credentials
        assertEquals("fallbackKey", credentials1.accessKeyId());
        assertEquals("fallbackKey", credentials2.accessKeyId());
        assertEquals("fallbackKey", credentials3.accessKeyId());

        // Verify Lake Formation was only called once (subsequent calls hit cache)
        verify(mockLfClient, times(1))
            .getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Test
    public void testCacheHitWithWritePermission() throws ExecutionException, InterruptedException {
        // Setup request with WRITE permission
        when(mockResolveIdentityRequest.property(PREFIX_PROPERTY)).thenReturn(TEST_S3_PREFIX);
        when(mockResolveIdentityRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.WRITE.toString());
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(mockResolveIdentityRequest);

        // Setup Lake Formation to return credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(TEST_S3_PREFIX))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        // Multiple calls with WRITE permission
        CompletableFuture<? extends AwsCredentialsIdentity> result1 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);
        CompletableFuture<? extends AwsCredentialsIdentity> result2 =
            identityProvider.resolveIdentity(mockResolveIdentityRequest);

        // Verify both calls return same credentials
        assertEquals(result1.get().accessKeyId(), result2.get().accessKeyId());
        assertEquals(TEST_ACCESS_KEY, result1.get().accessKeyId());
    }

    @Test
    public void testSeparateCacheForReadAndWritePermissions() throws ExecutionException, InterruptedException {
        // Setup READ request
        ResolveIdentityRequest readRequest = mock(ResolveIdentityRequest.class);
        when(readRequest.property(PREFIX_PROPERTY)).thenReturn(TEST_S3_PREFIX);
        when(readRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ.toString());
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(readRequest);

        // Setup WRITE request
        ResolveIdentityRequest writeRequest = mock(ResolveIdentityRequest.class);
        when(writeRequest.property(PREFIX_PROPERTY)).thenReturn(TEST_S3_PREFIX);
        when(writeRequest.property(PERMISSION_PROPERTY)).thenReturn(Permission.WRITE.toString());
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(writeRequest);

        // Setup Lake Formation to return credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(TEST_S3_PREFIX))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        // Call with READ permission twice
        identityProvider.resolveIdentity(readRequest).get();
        identityProvider.resolveIdentity(readRequest).get();

        // Call with WRITE permission twice
        identityProvider.resolveIdentity(writeRequest).get();
        identityProvider.resolveIdentity(writeRequest).get();

        // Verify Lake Formation was called twice (once for READ, once for WRITE)
        // because they have different cache keys
        verify(mockLfClient, times(2))
            .getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    }

    @Test
    public void testCacheHitForSamePrefixDifferentFiles() throws ExecutionException, InterruptedException {
        // Setup first request for file1
        ResolveIdentityRequest request1 = mock(ResolveIdentityRequest.class);
        when(request1.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/data/file1.txt");
        when(request1.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ.toString());
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(request1);

        // Setup second request for file2 (same prefix)
        ResolveIdentityRequest request2 = mock(ResolveIdentityRequest.class);
        when(request2.property(PREFIX_PROPERTY)).thenReturn("s3://test-bucket/data/file2.txt");
        when(request2.property(PERMISSION_PROPERTY)).thenReturn(Permission.READ.toString());
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(request2);

        // Setup Lake Formation to return credentials with wildcard prefix
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(TEST_ACCESS_KEY)
            .secretAccessKey(TEST_SECRET_KEY)
            .sessionToken(TEST_SESSION_TOKEN)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of("s3://test-bucket/data/*"))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        // First call for file1 - should hit Lake Formation
        AwsCredentialsIdentity credentials1 = identityProvider.resolveIdentity(request1).get();

        // Second call for file2 (same prefix) - should hit cache due to wildcard match
        AwsCredentialsIdentity credentials2 = identityProvider.resolveIdentity(request2).get();

        // Verify both calls return same cached credentials
        assertEquals(credentials1.accessKeyId(), credentials2.accessKeyId());
        assertEquals(TEST_ACCESS_KEY, credentials1.accessKeyId());
        assertEquals(TEST_ACCESS_KEY, credentials2.accessKeyId());
    }
}
