package software.amazon.lakeformation.plugin.accessgrants.plugin;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.utils.builder.CopyableBuilder;

public interface AccessGrantsPluginBuilder extends CopyableBuilder<AccessGrantsPluginBuilder, LakeFormationAccessGrantsPlugin> {
    AccessGrantsPluginBuilder enabled(@NotNull Boolean enabled);
    AccessGrantsPluginBuilder enableS3AccessGrantsFallback(@NotNull Boolean choice);
    AccessGrantsPluginBuilder enableDirectIAMFallback(@NotNull Boolean choice);
    AccessGrantsPluginBuilder userAgent(@NotNull String userAgent);

    /**
     * @deprecated Renamed to {@link #enableS3AccessGrantsFallback(Boolean)}. This forwarding
     *     method is retained for backward compatibility and will be removed in a future major
     *     version.
     */
    @Deprecated
    default AccessGrantsPluginBuilder enableFallback(@NotNull Boolean choice) {
        return enableS3AccessGrantsFallback(choice);
    }
}
