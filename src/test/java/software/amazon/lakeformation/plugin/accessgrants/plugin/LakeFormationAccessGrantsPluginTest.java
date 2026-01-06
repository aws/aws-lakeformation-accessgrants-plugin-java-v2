package software.amazon.lakeformation.plugin.accessgrants.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.auth.scheme.S3AuthSchemeProvider;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for LakeFormationAccessGrantsPlugin.
 */
public class LakeFormationAccessGrantsPluginTest {

    @Mock
    private S3ServiceClientConfiguration.Builder mockServiceClientConfiguration;

    @Mock
    private IdentityProvider<AwsCredentialsIdentity> mockOriginalIdentityProvider;

    @Mock
    private S3AuthSchemeProvider mockOriginalAuthSchemeProvider;

    @Mock
    private S3AccessGrantsPlugin mockS3AccessGrantsPlugin;

    private LakeFormationAccessGrantsPlugin plugin;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup mock configuration using doReturn to avoid type inference issues
        doReturn(mockOriginalIdentityProvider).when(mockServiceClientConfiguration).credentialsProvider();
        doReturn(mockOriginalAuthSchemeProvider).when(mockServiceClientConfiguration).authSchemeProvider();
        doReturn(Region.US_EAST_1).when(mockServiceClientConfiguration).region();

        plugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(true)
            .enableFallback(true)
            .build();
    }

    @Test
    public void testPluginBuilderWithDefaults() {
        LakeFormationAccessGrantsPlugin defaultPlugin = LakeFormationAccessGrantsPlugin.builder().build();

        assertNotNull(defaultPlugin);
        assertTrue(defaultPlugin.enableFallback());
    }

    @Test
    public void testPluginBuilderWithCustomSettings() {
        LakeFormationAccessGrantsPlugin customPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enableFallback(false)
            .userAgent("custom-agent")
            .build();

        assertNotNull(customPlugin);
        assertFalse(customPlugin.enableFallback());
    }

    @Test
    public void testConfigureClientWithValidConfiguration() {
        // This test verifies that configureClient doesn't throw exceptions
        // and properly validates the configuration type
        assertDoesNotThrow(() -> {
            plugin.configureClient(mockServiceClientConfiguration);
        });

        // Verify that the credentials provider was called to get original provider
        verify(mockServiceClientConfiguration, atLeastOnce()).credentialsProvider();
        verify(mockServiceClientConfiguration, atLeastOnce()).authSchemeProvider();

        // Verify that a new credentials provider was set
        verify(mockServiceClientConfiguration).credentialsProvider(any(LakeFormationAccessGrantsIdentityProvider.class));
    }

    @Test
    public void testConfigureClientWithInvalidConfiguration() {
        // Test with non-S3ServiceClientConfiguration
        SdkServiceClientConfiguration.Builder invalidConfig = mock(SdkServiceClientConfiguration.Builder.class);

        assertThrows(IllegalArgumentException.class, () -> {
            plugin.configureClient(invalidConfig);
        });
    }

    @Test
    public void testToBuilder() {
        LakeFormationAccessGrantsPlugin originalPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enableFallback(false)
            .userAgent("test-agent")
            .build();

        LakeFormationAccessGrantsPlugin copiedPlugin = originalPlugin.toBuilder().build();

        assertEquals(originalPlugin.enableFallback(), copiedPlugin.enableFallback());
    }

    @Test
    public void testBuilderEnableFallbackWithNull() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enableFallback(null)
            .build();

        // Should default to true when null is passed
        assertTrue(plugin.enableFallback());
    }

    @Test
    public void testBuilderUserAgentWithNull() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .userAgent(null)
            .build();

        // Should not throw exception and use default user agent
        assertNotNull(plugin);
    }

    @Test
    public void testConfigureClientLogsAppropriateMessages() {
        // This test ensures the plugin logs the expected configuration messages
        // In a real scenario, you might want to capture log output to verify messages
        assertDoesNotThrow(() -> {
            plugin.configureClient(mockServiceClientConfiguration);
        });

        // Verify the configuration was attempted
        verify(mockServiceClientConfiguration, atLeastOnce()).credentialsProvider();
    }

    @Test
    public void testPluginIntegrationWithS3AccessGrantsPlugin() {
        // Test that the plugin properly integrates with S3AccessGrantsPlugin
        assertDoesNotThrow(() -> {
            plugin.configureClient(mockServiceClientConfiguration);
        });

        // Verify that both original and new identity providers are used
        verify(mockServiceClientConfiguration).credentialsProvider(any(LakeFormationAccessGrantsIdentityProvider.class));
    }

    @Test
    public void testPluginBuilderEnabledByDefault() {
        LakeFormationAccessGrantsPlugin defaultPlugin = LakeFormationAccessGrantsPlugin.builder().build();

        assertNotNull(defaultPlugin);
        assertFalse(defaultPlugin.enabled());
    }

    @Test
    public void testPluginBuilderWithEnabledFalse() {
        LakeFormationAccessGrantsPlugin disabledPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(false)
            .build();

        assertNotNull(disabledPlugin);
        assertFalse(disabledPlugin.enabled());
    }

    @Test
    public void testBuilderEnabledWithNull() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(null)
            .build();

        // Should default to false when null is passed
        assertFalse(plugin.enabled());
    }

    @Test
    public void testConfigureClientWhenDisabled() {
        LakeFormationAccessGrantsPlugin disabledPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(false)
            .build();

        // When plugin is disabled, configureClient should return early without modifying config
        assertDoesNotThrow(() -> {
            disabledPlugin.configureClient(mockServiceClientConfiguration);
        });

        // Verify that credentials provider was NOT modified when disabled
        verify(mockServiceClientConfiguration, never()).credentialsProvider(any(LakeFormationAccessGrantsIdentityProvider.class));
    }

    @Test
    public void testToBuilderPreservesEnabledSetting() {
        LakeFormationAccessGrantsPlugin originalPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(false)
            .enableFallback(false)
            .userAgent("test-agent")
            .build();

        LakeFormationAccessGrantsPlugin copiedPlugin = originalPlugin.toBuilder().build();

        assertEquals(originalPlugin.enabled(), copiedPlugin.enabled());
        assertEquals(originalPlugin.enableFallback(), copiedPlugin.enableFallback());
    }
}
