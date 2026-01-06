package software.amazon.lakeformation.plugin.accessgrants.plugin;

import software.amazon.lakeformation.plugin.accessgrants.cache.AccessDeniedCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.AccessGrantsCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.CacheKey;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PERMISSION_PROPERTY;
import static software.amazon.awssdk.s3accessgrants.plugin.internal.S3AccessGrantsUtils.PREFIX_PROPERTY;

public class LakeFormationAccessGrantsIdentityProvider implements IdentityProvider<AwsCredentialsIdentity> {

    private static final Logger LOGGER = Logger.getLogger(LakeFormationAccessGrantsIdentityProvider.class.getName());

    private final IdentityProvider<? extends AwsCredentialsIdentity> originalProvider;
    private final LakeFormationClient lfClient;
    private final AccessDeniedCache accessDeniedCache;
    private final AccessGrantsCache accessGrantsCache;
    private final boolean enableFallback;
    private final IdentityProvider<? extends AwsCredentialsIdentity> s3AccessGrantsIdentityProvider;

    public LakeFormationAccessGrantsIdentityProvider(
            final IdentityProvider<? extends AwsCredentialsIdentity> originalProvider,
            final LakeFormationClient lfClient,
            final AccessDeniedCache accessDeniedCache,
            final AccessGrantsCache accessGrantsCache,
            final boolean enableFallback,
            final IdentityProvider<? extends AwsCredentialsIdentity> s3AccessGrantsIdentityProvider) {
        this.originalProvider = originalProvider;
        this.lfClient = lfClient;
        this.accessDeniedCache = accessDeniedCache;
        this.accessGrantsCache = accessGrantsCache;
        this.enableFallback = enableFallback;
        this.s3AccessGrantsIdentityProvider = s3AccessGrantsIdentityProvider;
    }

    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return AwsCredentialsIdentity.class;
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(
            final ResolveIdentityRequest resolveIdentityRequest) {
        try {
            // Get requester credentials
            final AwsCredentialsIdentity requesterCredentials = originalProvider
                    .resolveIdentity(resolveIdentityRequest).get();

            // Get S3 prefix from request properties
            final String s3Prefix = resolveIdentityRequest.property(PREFIX_PROPERTY).toString();
            final Permission permission = Permission.fromValue(resolveIdentityRequest.property(PERMISSION_PROPERTY)
                    .toString());

            // Construct cache key
            final CacheKey cacheKey = new CacheKey(requesterCredentials, permission, s3Prefix);

            // Check access denied cache first
            Exception accessDeniedException = accessDeniedCache.getValueFromCache(cacheKey);
            if (accessDeniedException != null) {
                LOGGER.info("Found cached Access Denied Exception: " + accessDeniedException.getMessage());
                throw new RuntimeException(accessDeniedException);
            }

            // Get Lake Formation credentials
            final AwsCredentials lfTempCredentials = accessGrantsCache.getCredentials(
                    lfClient, cacheKey, accessDeniedCache);

            LOGGER.info("Successfully resolved Lake Formation credentials");
            if (lfTempCredentials instanceof AwsSessionCredentials) {
                return CompletableFuture.completedFuture(AwsSessionCredentials.create(
                        lfTempCredentials.accessKeyId(),
                        lfTempCredentials.secretAccessKey(),
                        ((AwsSessionCredentials) lfTempCredentials).sessionToken()));
            }
            return CompletableFuture.completedFuture(AwsCredentialsIdentity.create(
                    lfTempCredentials.accessKeyId(), lfTempCredentials.secretAccessKey()));
        } catch (Exception e) {
            LOGGER.info("Lake Formation exception: " + e.getMessage());
            if (enableFallback && s3AccessGrantsIdentityProvider != null) {
                LOGGER.info("Falling back to S3AccessGrant credential provider");
                return s3AccessGrantsIdentityProvider.resolveIdentity(resolveIdentityRequest);
            } else {
                return CompletableFuture.failedFuture(
                    SdkClientException.create("Failed to resolve Lake Formation credentials", e));
            }
        }
    }
}
