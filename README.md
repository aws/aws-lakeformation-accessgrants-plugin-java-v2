# AWS Lake Formation Access Grants Plugin for Java v2

A Java implementation of the Lake Formation Access Grants Plugin for AWS S3 Access Grants integration with Lake Formation.

## Overview

This plugin provides seamless integration between S3 operations and AWS Lake Formation access grants, enabling fine-grained access control for data stored in S3 through Lake Formation permissions.

## Features

- **Access Grants Resolution**: Automatically resolves Lake Formation temporary credentials for S3 operations
- **Intelligent Caching**: Implements multi-level caching for both successful credentials and access denied responses using Caffeine cache
- **Fallback Support**: Falls back to S3 Access Grants when Lake Formation access is denied
- **Operation Mapping**: Maps S3 operations to appropriate permissions (READ, WRITE, READWRITE) using S3AccessGrantsStaticOperationToPermissionMapper
- **SdkPlugin Integration**: Integrates with AWS SDK v2 using SdkPlugin interface for seamless client configuration

## Core Components

### LakeFormationAccessGrantsPlugin
Main plugin class that implements `SdkPlugin` and handles:
- Integration with S3AccessGrantsPlugin internally
- Client configuration through `configureClient()` method
- Fallback mechanism configuration

### LakeFormationAccessGrantsIdentityProvider
Custom identity provider that handles:
- Lake Formation credential resolution
- Fallback to S3 Access Grants when access is denied
- Integration with caching system

### Cache System
- **AccessGrantsCache**: Caches temporary credentials with TTL-based expiration using Caffeine
- **AccessDeniedCache**: Caches access denied responses to avoid repeated failed requests using Caffeine
- **CacheKey**: Composite key for cache operations based on credentials, permissions, and S3 prefix

## Usage

```java
import software.amazon.lakeformation.plugin.accessgrants.plugin.LakeFormationAccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3Client;

// Initialize S3 client with plugin
// Note: The plugin is DISABLED by default - you must explicitly enable it
S3Client s3Client = S3Client.builder()
        .region(Region.US_WEST_2)
        .addPlugin(LakeFormationAccessGrantsPlugin.builder()
                .enabled(true)  // Required: plugin is disabled by default
                .enableFallback(true)
                .build())
        .build();

// Use s3Client normally - plugin will automatically intercept and resolve credentials
s3Client.getObject(GetObjectRequest.builder()
        .bucket("my-bucket")
        .key("my-key")
        .build());
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable/disable the plugin. When disabled, the plugin skips all Lake Formation configuration and uses original credentials. |
| `enableFallback` | `true` | When enabled, falls back to S3 Access Grants (then IAM) if Lake Formation access is denied. |

## Architecture

The plugin works by:

1. **Registration**: The plugin registers as an `SdkPlugin` with the S3Client during construction
2. **Configuration**: The `configureClient()` method sets up the identity provider and integrates with S3AccessGrantsPlugin
3. **Credential Resolution**: The identity provider extracts operation type and S3 URI, then checks caches for existing credentials
4. **Lake Formation Integration**: If not cached, calls Lake Formation `getTemporaryDataLocationCredentials`
5. **Credential Injection**: Returns Lake Formation temporary credentials or falls back to S3 Access Grants
6. **Caching**: Caches both successful credentials and access denied exceptions for future requests

## Testing

The package includes comprehensive unit tests:
- `LakeFormationAccessGrantsPluginTest`: Tests plugin functionality with mocked Lake Formation client
- `LakeFormationAccessGrantsIdentityProviderTest`: Tests identity provider credential resolution
- `AccessGrantsCacheTest`: Tests credential caching behavior
- `AccessDeniedCacheTest`: Tests access denied exception caching
- `CacheKeyTest`: Tests cache key equality and validation

Run tests with:
```bash
mvn test
```

## Building

```bash
mvn clean compile
mvn test-compile
```

## Dependencies

- AWS SDK for Java v2 (S3, Lake Formation)
- S3 Access Grants Plugin for operation-to-permission mapping
- Caffeine cache for high-performance caching
- JUnit 5 and Mockito for testing

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

