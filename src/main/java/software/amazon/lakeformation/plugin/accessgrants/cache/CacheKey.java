package software.amazon.lakeformation.plugin.accessgrants.cache;

import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.util.Objects;

/**
 * Cache key for storing access grants credentials.
 */
public class CacheKey {
    private final AwsCredentialsIdentity credentials;
    private final Permission permission;
    private final String s3Prefix;

    public CacheKey(AwsCredentialsIdentity credentials, Permission permission, String s3Prefix) {
        this.credentials = credentials;
        this.permission = permission;
        this.s3Prefix = s3Prefix;

        if (credentials == null || permission == null || s3Prefix == null) {
            throw new IllegalArgumentException("Credentials, permission, and s3_prefix must be provided");
        }
    }

    public CacheKey(CacheKey cacheKey, Permission permission) {
        this.credentials = cacheKey.credentials;
        this.s3Prefix = cacheKey.s3Prefix;
        this.permission = permission;

        if (this.credentials == null || this.permission == null || this.s3Prefix == null) {
            throw new IllegalArgumentException("Credentials, permission, and s3_prefix must be provided");
        }
    }

    public CacheKey(CacheKey cacheKey, Permission permission, String s3Prefix) {
        if (permission != null) {
            this.credentials = cacheKey.credentials;
            this.s3Prefix = cacheKey.s3Prefix;
            this.permission = permission;
        } else if (s3Prefix != null) {
            this.credentials = cacheKey.credentials;
            this.permission = cacheKey.permission;
            this.s3Prefix = s3Prefix;
        } else {
            this.credentials = cacheKey.credentials;
            this.permission = cacheKey.permission;
            this.s3Prefix = cacheKey.s3Prefix;
        }

        if (this.credentials == null || this.permission == null || this.s3Prefix == null) {
            throw new IllegalArgumentException("Credentials, permission, and s3_prefix must be provided");
        }
    }

    public AwsCredentialsIdentity getCredentials() {
        return credentials;
    }

    public Permission getPermission() {
        return permission;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CacheKey)) {
            return false;
        }
        CacheKey cacheKey = (CacheKey) obj;
        return Objects.equals(credentials.accessKeyId(), cacheKey.credentials.accessKeyId())
               && Objects.equals(credentials.secretAccessKey(), cacheKey.credentials.secretAccessKey())
               && Objects.equals(permission, cacheKey.permission)
               && Objects.equals(s3Prefix, cacheKey.s3Prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentials.accessKeyId(), credentials.secretAccessKey(), permission, s3Prefix);
    }
}
