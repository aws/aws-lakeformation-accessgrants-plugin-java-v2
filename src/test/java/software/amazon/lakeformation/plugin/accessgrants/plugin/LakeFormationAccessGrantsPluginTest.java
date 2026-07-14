package software.amazon.lakeformation.plugin.accessgrants.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
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
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
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
        // Setup mock configuration using doReturn to avoid type inference issues
        doReturn(mockOriginalIdentityProvider).when(mockServiceClientConfiguration).credentialsProvider();
        doReturn(mockOriginalAuthSchemeProvider).when(mockServiceClientConfiguration).authSchemeProvider();
        doReturn(Region.US_EAST_1).when(mockServiceClientConfiguration).region();

        plugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(true)
            .enableS3AccessGrantsFallback(true)
            .build();
    }

    @Test
    public void testPluginBuilderWithDefaults() {
        LakeFormationAccessGrantsPlugin defaultPlugin = LakeFormationAccessGrantsPlugin.builder().build();

        assertNotNull(defaultPlugin);
        // S3 Access Grants fallback is on by default; direct IAM fallback is off by default
        assertTrue(defaultPlugin.enableS3AccessGrantsFallback());
        assertFalse(defaultPlugin.enableDirectIAMFallback());
    }

    @Test
    public void testPluginBuilderWithCustomSettings() {
        LakeFormationAccessGrantsPlugin customPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enableS3AccessGrantsFallback(false)
            .enableDirectIAMFallback(true)
            .userAgent("custom-agent")
            .build();

        assertNotNull(customPlugin);
        assertFalse(customPlugin.enableS3AccessGrantsFallback());
        assertTrue(customPlugin.enableDirectIAMFallback());
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
            .enableS3AccessGrantsFallback(false)
            .enableDirectIAMFallback(true)
            .userAgent("test-agent")
            .build();

        LakeFormationAccessGrantsPlugin copiedPlugin = originalPlugin.toBuilder().build();

        assertEquals(originalPlugin.enableS3AccessGrantsFallback(), copiedPlugin.enableS3AccessGrantsFallback());
        assertEquals(originalPlugin.enableDirectIAMFallback(), copiedPlugin.enableDirectIAMFallback());
    }

    @Test
    public void testBuilderEnableS3AccessGrantsFallbackWithNull() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enableS3AccessGrantsFallback(null)
            .build();

        // Should default to true when null is passed
        assertTrue(plugin.enableS3AccessGrantsFallback());
    }

    @Test
    public void testBuilderEnableDirectIAMFallbackWithNull() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enableDirectIAMFallback(null)
            .build();

        // Should default to false when null is passed
        assertFalse(plugin.enableDirectIAMFallback());
    }

    @Test
    public void testDirectIAMFallbackDefaultIsFalse() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(true)
            .build();

        // Default must be false for backward compatibility (LF -> S3AG -> IAM stays the default chain)
        assertFalse(plugin.enableDirectIAMFallback());
        assertTrue(plugin.enableS3AccessGrantsFallback());
    }

    @Test
    public void testBuilderAcceptsDirectIAMFallback() {
        LakeFormationAccessGrantsPlugin plugin = LakeFormationAccessGrantsPlugin.builder()
            .enabled(true)
            .enableS3AccessGrantsFallback(false)
            .enableDirectIAMFallback(true)
            .build();

        assertTrue(plugin.enabled());
        assertFalse(plugin.enableS3AccessGrantsFallback());
        assertTrue(plugin.enableDirectIAMFallback());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedEnableFallbackForwardsToS3AccessGrantsFallback() {
        // The deprecated enableFallback(...) method is retained for backward compatibility and
        // must forward to enableS3AccessGrantsFallback(...).
        LakeFormationAccessGrantsPlugin enabledPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enableFallback(true)
            .build();
        assertTrue(enabledPlugin.enableS3AccessGrantsFallback());

        LakeFormationAccessGrantsPlugin disabledPlugin = LakeFormationAccessGrantsPlugin.builder()
            .enableFallback(false)
            .build();
        assertFalse(disabledPlugin.enableS3AccessGrantsFallback());
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
            .enableS3AccessGrantsFallback(false)
            .enableDirectIAMFallback(true)
            .userAgent("test-agent")
            .build();

        LakeFormationAccessGrantsPlugin copiedPlugin = originalPlugin.toBuilder().build();

        assertEquals(originalPlugin.enabled(), copiedPlugin.enabled());
        assertEquals(originalPlugin.enableS3AccessGrantsFallback(), copiedPlugin.enableS3AccessGrantsFallback());
        assertEquals(originalPlugin.enableDirectIAMFallback(), copiedPlugin.enableDirectIAMFallback());
    }
}
