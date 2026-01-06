package software.amazon.lakeformation.plugin.accessgrants.plugin;

import software.amazon.awssdk.annotations.NotNull;
import software.amazon.awssdk.utils.builder.CopyableBuilder;

public interface AccessGrantsPluginBuilder extends CopyableBuilder<AccessGrantsPluginBuilder, LakeFormationAccessGrantsPlugin> {
    AccessGrantsPluginBuilder enabled(@NotNull Boolean enabled);
    AccessGrantsPluginBuilder enableFallback(@NotNull Boolean choice);
    AccessGrantsPluginBuilder userAgent(@NotNull String userAgent);
}
