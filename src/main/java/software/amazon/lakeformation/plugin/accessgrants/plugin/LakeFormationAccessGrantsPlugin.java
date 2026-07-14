package software.amazon.lakeformation.plugin.accessgrants.plugin;

import software.amazon.lakeformation.plugin.accessgrants.cache.AccessDeniedCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.AccessGrantsCache;
import software.amazon.lakeformation.plugin.accessgrants.cache.ExceptionCache;
import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

import java.util.logging.Logger;

/**
 * Lake Formation Access Grants Plugin for S3 operations with Lake Formation integration.
 */
public class LakeFormationAccessGrantsPlugin implements SdkPlugin,
        ToCopyableBuilder<AccessGrantsPluginBuilder, LakeFormationAccessGrantsPlugin> {

    private static final Logger LOGGER = Logger.getLogger(LakeFormationAccessGrantsPlugin.class.getName());
    private static final String USER_AGENT = "lakeformation-access-grants-plugin";
    private static final boolean DEFAULT_S3_ACCESS_GRANTS_FALLBACK_SETTING = true;
    private static final boolean DEFAULT_DIRECT_IAM_FALLBACK_SETTING = false;
    private static final boolean DEFAULT_ENABLED_SETTING = false;

    private final boolean enabled;
    private final boolean enableS3AccessGrantsFallback;
    private final boolean enableDirectIAMFallback;
    private final String userAgent;

    LakeFormationAccessGrantsPlugin(final BuilderImpl builder) {
        this.enabled = builder.enabled;
        this.enableS3AccessGrantsFallback = builder.enableS3AccessGrantsFallback;
        this.enableDirectIAMFallback = builder.enableDirectIAMFallback;
        this.userAgent = builder.userAgent;
    }

    public static AccessGrantsPluginBuilder builder() {
        return new BuilderImpl();
    }

    boolean enabled() {
        return this.enabled;
    }

    /**
     * Returns whether the S3 Access Grants fallback is enabled.
     * When true, Lake Formation failures fall back to S3 Access Grants (which in turn falls back to IAM).
     */
    boolean enableS3AccessGrantsFallback() {
        return this.enableS3AccessGrantsFallback;
    }

    /**
     * Returns whether the direct IAM fallback is enabled.
     * When true (and S3 Access Grants fallback is not used), Lake Formation failures fall back
     * directly to the original IAM credentials provider, skipping S3 Access Grants.
     */
    boolean enableDirectIAMFallback() {
        return this.enableDirectIAMFallback;
    }

    @Override
    public void configureClient(final SdkServiceClientConfiguration.Builder config) {
        if (!enabled()) {
            LOGGER.info("Lake Formation Access Grants Plugin is disabled. Skipping Lake Formation configuration.");
            return;
        }

        LOGGER.info("Configuring S3 Clients to use Lake Formation as a permission layer!");
        LOGGER.info("Running the Lake Formation Access grants plugin with S3 Access Grants fallback enabled: "
                + enableS3AccessGrantsFallback() + ", direct IAM fallback enabled: " + enableDirectIAMFallback());

        if (!enableS3AccessGrantsFallback() && !enableDirectIAMFallback()) {
            LOGGER.warning("No fallback opted in! S3 Client will not fall back to evaluate policies if "
                    + "permissions are not provided through Lake Formation!");
        } else if (enableS3AccessGrantsFallback()) {
            LOGGER.info("Lake Formation failures will fall back to S3 Access Grants (then IAM).");
        } else {
            LOGGER.info("Lake Formation failures will fall back directly to the original credentials provider "
                    + "(IAM), skipping S3 Access Grants.");
        }

        final S3ServiceClientConfiguration.Builder serviceClientConfiguration =
            Validate.isInstanceOf(S3ServiceClientConfiguration.Builder.class,
                config,
                "Expecting the plugin to be only configured on s3 clients");

        final String region = serviceClientConfiguration.region().toString();
        final LakeFormationClient lfClient = LakeFormationClient.builder()
                .region(Region.of(region))
                .build();
        LOGGER.info("Initializing caches...");
        final AccessDeniedCache accessDeniedCache = new AccessDeniedCache();
        final AccessGrantsCache accessGrantsCache = new AccessGrantsCache();
        final ExceptionCache exceptionCache = new ExceptionCache();

        final IdentityProvider<? extends AwsCredentialsIdentity> originalIdentityProvider = serviceClientConfiguration
                .credentialsProvider();

        // Configure S3AccessGrantsPlugin to get its identity provider
        final S3AccessGrantsPlugin s3AccessGrantsPlugin = S3AccessGrantsPlugin.builder()
                .enableFallback(enableS3AccessGrantsFallback)
                .build();

        s3AccessGrantsPlugin.configureClient(config);
        S3ServiceClientConfiguration.Builder s3AccessGrantClientConfig =
            Validate.isInstanceOf(S3ServiceClientConfiguration.Builder.class,
                config,
                "Expecting the plugin to be only configured on s3 clients");

        serviceClientConfiguration.credentialsProvider(new LakeFormationAccessGrantsIdentityProvider(
            originalIdentityProvider,
            lfClient,
            accessDeniedCache,
            accessGrantsCache,
            exceptionCache,
            enableS3AccessGrantsFallback,
            enableDirectIAMFallback,
            s3AccessGrantClientConfig.credentialsProvider()
        ));

        LOGGER.info("Completed configuring S3 Clients to use Lake Formation as a permission layer!");
    }

    @Override
    public AccessGrantsPluginBuilder toBuilder() {
        return new BuilderImpl(this);
    }

    public static final class BuilderImpl implements AccessGrantsPluginBuilder {
        private boolean enabled;
        private boolean enableS3AccessGrantsFallback;
        private boolean enableDirectIAMFallback;
        private String userAgent;

        BuilderImpl() {
            this.enabled = DEFAULT_ENABLED_SETTING;
            this.enableS3AccessGrantsFallback = DEFAULT_S3_ACCESS_GRANTS_FALLBACK_SETTING;
            this.enableDirectIAMFallback = DEFAULT_DIRECT_IAM_FALLBACK_SETTING;
            this.userAgent = USER_AGENT;
        }

        BuilderImpl(LakeFormationAccessGrantsPlugin plugin) {
            this.enabled = plugin.enabled;
            this.enableS3AccessGrantsFallback = plugin.enableS3AccessGrantsFallback;
            this.enableDirectIAMFallback = plugin.enableDirectIAMFallback;
            this.userAgent = plugin.userAgent;
        }

        @Override
        public LakeFormationAccessGrantsPlugin build() {
            return new LakeFormationAccessGrantsPlugin(this);
        }

        @Override
        public AccessGrantsPluginBuilder enabled(@NotNull Boolean enabled) {
            this.enabled = enabled == null ? DEFAULT_ENABLED_SETTING : enabled;
            return this;
        }

        @Override
        public AccessGrantsPluginBuilder enableS3AccessGrantsFallback(@NotNull Boolean choice) {
            this.enableS3AccessGrantsFallback = choice == null
                    ? DEFAULT_S3_ACCESS_GRANTS_FALLBACK_SETTING : choice;
            return this;
        }

        @Override
        public AccessGrantsPluginBuilder enableDirectIAMFallback(@NotNull Boolean choice) {
            this.enableDirectIAMFallback = choice == null
                    ? DEFAULT_DIRECT_IAM_FALLBACK_SETTING : choice;
            return this;
        }

        @Override
        public AccessGrantsPluginBuilder userAgent(@NotNull String userAgent) {
            if (userAgent == null) {
                this.userAgent = USER_AGENT;
            } else {
                this.userAgent = USER_AGENT + "-" + userAgent;
            }
            return this;
        }
    }
}
