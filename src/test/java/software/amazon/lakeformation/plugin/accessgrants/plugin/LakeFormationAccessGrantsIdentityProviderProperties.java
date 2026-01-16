package software.amazon.lakeformation.plugin.accessgrants.plugin;

import net.jqwik.api.*;
import org.mockito.Mockito;
import software.amazon.lakeformation.plugin.accessgrants.cache.AccessDeniedCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.AccessGrantsCache;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

/**
 * Property-based tests for LakeFormationAccessGrantsIdentityProvider.
 * Feature: lakeformation-api-integration
 */
public class LakeFormationAccessGrantsIdentityProviderProperties {

    /**
     * Fallback Behavior
     * For any Lake Formation credential resolution failure when fallback is enabled,
     * the S3 Access Grants identity provider should be called; when fallback is disabled,
     * an SdkClientException should be returned.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Property 6: Fallback Behavior")
    void fallbackBehavior(
            @ForAll("validS3Prefixes") String s3Prefix,
            @ForAll("validPermissions") Permission permission,
            @ForAll boolean enableFallback) throws ExecutionException, InterruptedException {

        // Setup mocks
        @SuppressWarnings("unchecked")
        IdentityProvider<AwsCredentialsIdentity> mockOriginalProvider = Mockito.mock(IdentityProvider.class);
        LakeFormationClient mockLfClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache accessDeniedCache = new AccessDeniedCache();
        AccessGrantsCache accessGrantsCache = new AccessGrantsCache();
        @SuppressWarnings("unchecked")
        IdentityProvider<AwsCredentialsIdentity> mockS3AccessGrantsProvider = Mockito.mock(IdentityProvider.class);
        ResolveIdentityRequest mockRequest = Mockito.mock(ResolveIdentityRequest.class);

        AwsCredentialsIdentity testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Setup original provider
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(mockRequest);

        // Setup request properties
        when(mockRequest.property(PREFIX_PROPERTY)).thenReturn(s3Prefix);
        when(mockRequest.property(PERMISSION_PROPERTY)).thenReturn(permission.toString());

        // Setup Lake Formation to fail
        RuntimeException lfException = new RuntimeException("Lake Formation service error");
        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenThrow(lfException);

        // Setup fallback provider
        AwsCredentialsIdentity fallbackCredentials = AwsBasicCredentials.create("fallbackKey", "fallbackSecret");
        doReturn(CompletableFuture.completedFuture(fallbackCredentials))
            .when(mockS3AccessGrantsProvider).resolveIdentity(mockRequest);

        // Create identity provider with specified fallback setting
        LakeFormationAccessGrantsIdentityProvider identityProvider = new LakeFormationAccessGrantsIdentityProvider(
            mockOriginalProvider,
            mockLfClient,
            accessDeniedCache,
            accessGrantsCache,
            enableFallback,
            mockS3AccessGrantsProvider
        );

        CompletableFuture<? extends AwsCredentialsIdentity> result = identityProvider.resolveIdentity(mockRequest);

        if (enableFallback) {
            // When fallback is enabled, should return fallback credentials
            AwsCredentialsIdentity resolvedCredentials = result.get();
            assert "fallbackKey".equals(resolvedCredentials.accessKeyId()) : "Should return fallback credentials";
            verify(mockS3AccessGrantsProvider).resolveIdentity(mockRequest);
        } else {
            // When fallback is disabled, should fail with SdkClientException
            assert result.isCompletedExceptionally() : "Should fail when fallback is disabled";
            try {
                result.get();
                assert false : "Should have thrown exception";
            } catch (ExecutionException e) {
                assert e.getCause() instanceof SdkClientException : "Should be SdkClientException";
                assert e.getCause().getMessage().contains("Failed to resolve Lake Formation credentials") :
                    "Error message should indicate Lake Formation failure";
            }
            verify(mockS3AccessGrantsProvider, never()).resolveIdentity(any(ResolveIdentityRequest.class));
        }
    }

    /**
     * Session Credentials Return Type
     * For any successful Lake Formation credential resolution, the returned credentials
     * should be of type AwsSessionCredentials containing access key ID, secret access key,
     * and session token.
     */
    @Property(tries = 100)
    @Label("Feature: lakeformation-api-integration, Property 7: Session Credentials Return Type")
    void sessionCredentialsReturnType(
            @ForAll("validS3Prefixes") String s3Prefix,
            @ForAll("validPermissions") Permission permission,
            @ForAll("validCredentialComponents") String accessKeyId,
            @ForAll("validCredentialComponents") String secretAccessKey,
            @ForAll("validCredentialComponents") String sessionToken) throws ExecutionException, InterruptedException {

        // Setup mocks
        @SuppressWarnings("unchecked")
        IdentityProvider<AwsCredentialsIdentity> mockOriginalProvider = Mockito.mock(IdentityProvider.class);
        LakeFormationClient mockLfClient = Mockito.mock(LakeFormationClient.class);
        AccessDeniedCache accessDeniedCache = new AccessDeniedCache();
        AccessGrantsCache accessGrantsCache = new AccessGrantsCache();
        @SuppressWarnings("unchecked")
        IdentityProvider<AwsCredentialsIdentity> mockS3AccessGrantsProvider = Mockito.mock(IdentityProvider.class);
        ResolveIdentityRequest mockRequest = Mockito.mock(ResolveIdentityRequest.class);

        AwsCredentialsIdentity testCredentials = AwsBasicCredentials.create("accessKey", "secretKey");

        // Setup original provider
        doReturn(CompletableFuture.completedFuture(testCredentials))
            .when(mockOriginalProvider).resolveIdentity(mockRequest);

        // Setup request properties
        when(mockRequest.property(PREFIX_PROPERTY)).thenReturn(s3Prefix);
        when(mockRequest.property(PERMISSION_PROPERTY)).thenReturn(permission.toString());

        // Setup Lake Formation to return session credentials
        TemporaryCredentials tempCreds = TemporaryCredentials.builder()
            .accessKeyId(accessKeyId)
            .secretAccessKey(secretAccessKey)
            .sessionToken(sessionToken)
            .build();

        GetTemporaryDataLocationCredentialsResponse response = GetTemporaryDataLocationCredentialsResponse.builder()
            .credentials(tempCreds)
            .accessibleDataLocations(List.of(s3Prefix))
            .build();

        when(mockLfClient.getTemporaryDataLocationCredentials(any(GetTemporaryDataLocationCredentialsRequest.class)))
            .thenReturn(response);

        // Create identity provider
        LakeFormationAccessGrantsIdentityProvider identityProvider = new LakeFormationAccessGrantsIdentityProvider(
            mockOriginalProvider,
            mockLfClient,
            accessDeniedCache,
            accessGrantsCache,
            true,
            mockS3AccessGrantsProvider
        );

        CompletableFuture<? extends AwsCredentialsIdentity> result = identityProvider.resolveIdentity(mockRequest);
        AwsCredentialsIdentity resolvedCredentials = result.get();

        // Verify it's an AwsSessionCredentials instance
        assert resolvedCredentials instanceof AwsSessionCredentials :
            "Resolved credentials should be AwsSessionCredentials";

        AwsSessionCredentials sessionCreds = (AwsSessionCredentials) resolvedCredentials;

        // Verify all credential components are present and match
        assert accessKeyId.equals(sessionCreds.accessKeyId()) : "Access key ID should match";
        assert secretAccessKey.equals(sessionCreds.secretAccessKey()) : "Secret access key should match";
        assert sessionToken.equals(sessionCreds.sessionToken()) : "Session token should match";
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
    Arbitrary<String> validCredentialComponents() {
        return Arbitraries.strings().alpha().numeric().ofMinLength(10).ofMaxLength(40);
    }
}
