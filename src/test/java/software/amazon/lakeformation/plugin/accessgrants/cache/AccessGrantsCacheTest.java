package software.amazon.lakeformation.plugin.accessgrants.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
// TODO: Uncomment when Lake Formation API classes are available
// import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
// import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
// import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
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

    // TODO: Uncomment and update when Lake Formation API classes are available
    // @Test
    // public void testGetCredentialsFromLakeFormation() {
    //     // Mock successful LakeFormation response
    //     GetTemporaryDataLocationCredentialsResponse mockResponse = GetTemporaryDataLocationCredentialsResponse.builder()
    //         .credentials(TemporaryCredentials.builder()
    //             .accessKeyId("tempAccessKey")
    //             .secretAccessKey("tempSecretKey")
    //             .sessionToken("tempSessionToken")
    //             .build())
    //         .accessibleDataLocations(List.of("s3://test-bucket/test-key"))
    //         .build();

    //     when(mockLakeFormationClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
    //         .thenReturn(mockResponse);

    //     AwsCredentials result = cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);

    //     assertNotNull(result);
    //     verify(mockLakeFormationClient).getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class));
    //     assertEquals("tempAccessKey", result.accessKeyId());
    //     assertEquals("tempSecretKey", result.secretAccessKey());
    // }

    @Test
    public void testGetCredentialsThrowsUnsupportedOperation() {
        // Since Lake Formation API is not yet implemented, expect UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> {
            cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        });
    }

    @Test
    public void testGetCredentialsHandlesAccessDenied() {
        // Mock AccessDenied exception
        AwsErrorDetails errorDetails = AwsErrorDetails.builder()
            .errorCode("AccessDenied")
            .build();
        AwsServiceException accessDeniedException = LakeFormationException.builder()
            .awsErrorDetails(errorDetails)
            .build();

        // Since the current implementation throws UnsupportedOperationException,
        // we can't test the AccessDenied handling until Lake Formation API is implemented
        // TODO: Update this test when Lake Formation integration is complete

        assertThrows(UnsupportedOperationException.class, () -> {
            cache.getCredentials(mockLakeFormationClient, testKey, mockAccessDeniedCache);
        });
    }
}
