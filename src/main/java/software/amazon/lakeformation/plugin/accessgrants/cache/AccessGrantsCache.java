package software.amazon.lakeformation.plugin.accessgrants.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
// TODO: Verify these Lake Formation model classes exist in the current AWS SDK version
// import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsRequest;
// import software.amazon.awssdk.services.lakeformation.model.GetTemporaryDataLocationCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
// import software.amazon.awssdk.services.lakeformation.model.TemporaryCredentials;
import software.amazon.awssdk.services.s3control.model.Permission;

import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Cache for storing access grants credentials.
 */
public class AccessGrantsCache {
    private static final Logger LOGGER = Logger.getLogger(AccessGrantsCache.class.getName());

    private static final int DEFAULT_ACCESS_GRANTS_CACHE_SIZE = 30000;
    private static final int MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE = 1000000;
    private static final int GET_DATA_ACCESS_DURATION = 1 * 60 * 60; // 1 hour
    private static final int MAX_GET_DATA_ACCESS_DURATION = 12 * 60 * 60; // 12 hours
    private static final int CACHE_EXPIRATION_TIME_PERCENTAGE = 90;

    private final Cache<CacheKey, AwsCredentials> accessGrantsCache;

    public AccessGrantsCache() {
        this(DEFAULT_ACCESS_GRANTS_CACHE_SIZE, GET_DATA_ACCESS_DURATION);
    }

    public AccessGrantsCache(final int cacheSize, final int duration) {
        if (cacheSize > MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE) {
            throw new IllegalArgumentException(
                "Max cache size should be less than or equal to " + MAX_LIMIT_ACCESS_GRANTS_CACHE_SIZE);
        }

        if (duration > MAX_GET_DATA_ACCESS_DURATION) {
            throw new IllegalArgumentException(
                "Maximum duration should be less than or equal to " + MAX_GET_DATA_ACCESS_DURATION);
        }

        long cacheTtl = ((long) duration * CACHE_EXPIRATION_TIME_PERCENTAGE) / 100;
        this.accessGrantsCache = Caffeine.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(Duration.ofSeconds(cacheTtl))
            .build();
    }

    private AwsCredentials searchCredentialsAtPrefixLevel(final CacheKey cacheKey) {
        String prefix = cacheKey.getS3Prefix();
        while (!prefix.equals("s3:")) {
            final CacheKey searchKey = new CacheKey(cacheKey, null, prefix);
            final AwsCredentials cacheValue = accessGrantsCache.getIfPresent(searchKey);
            if (cacheValue != null) {
                LOGGER.info("Successfully retrieved credentials from cache.");
                return cacheValue;
            }
            final int lastSlash = prefix.lastIndexOf("/");
            if (lastSlash == -1) break;
            prefix = prefix.substring(0, lastSlash);
        }
        return null;
    }

    private AwsCredentials searchCredentialsAtCharacterLevel(final CacheKey cacheKey) {
        String prefix = cacheKey.getS3Prefix();
        while (!prefix.equals("s3://")) {
            final CacheKey searchKey = new CacheKey(cacheKey, null, prefix + "*");
            final AwsCredentials cacheValue = accessGrantsCache.getIfPresent(searchKey);
            if (cacheValue != null) {
                LOGGER.info("Successfully retrieved credentials from cache.");
                return cacheValue;
            }
            if (prefix.length() <= 1) break;
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return null;
    }

    private Object getCredentialsFromLfService(
            final LakeFormationClient lfClient,
            final CacheKey cacheKey) {
        if (lfClient == null) {
            throw new RuntimeException("Unknown error occurred when initializing LakeFormation client");
        }
        LOGGER.info("Fetching credentials from Lake Formation for s3Prefix: " + cacheKey.getS3Prefix()
            + ", permission: " + cacheKey.getPermission());

        // TODO: Implement Lake Formation API call when classes are available
        // return lfClient.getTemporaryDataLocationCredentials(
        //     GetTemporaryDataLocationCredentialsRequest.builder()
        //         .dataLocations(cacheKey.getS3Prefix())
        //         .build()
        // );
        throw new UnsupportedOperationException("Lake Formation API integration pending");
    }

    private String processMatchedTarget(final String matchedGrantTarget) {
        if (matchedGrantTarget.endsWith("/*")) {
            return matchedGrantTarget.substring(0, matchedGrantTarget.length() - 2);
        } else if (matchedGrantTarget.endsWith("/")) {
            return matchedGrantTarget.substring(0, matchedGrantTarget.length() - 1);
        }
        return matchedGrantTarget;
    }

    public AwsCredentials getCredentials(
            final LakeFormationClient lfClient,
            final CacheKey cacheKey,
            final AccessDeniedCache accessDeniedCache) {
        LOGGER.info("Fetching credentials from LakeFormation for s3Prefix: " + cacheKey.getS3Prefix());
        AwsCredentials credentials = searchCredentialsAtPrefixLevel(cacheKey);
        if (credentials == null && (Permission.READ.equals(cacheKey.getPermission())
                || Permission.WRITE.equals(cacheKey.getPermission()))) {
            credentials = searchCredentialsAtPrefixLevel(new CacheKey(cacheKey, Permission.READWRITE));
        }
        if (credentials == null) {
            credentials = searchCredentialsAtCharacterLevel(cacheKey);
        }
        if (credentials == null && (Permission.READ.equals(cacheKey.getPermission())
                || Permission.WRITE.equals(cacheKey.getPermission()))) {
            credentials = searchCredentialsAtCharacterLevel(new CacheKey(cacheKey, Permission.READWRITE));
        }
        if (credentials == null) {
            LOGGER.info("Credentials not available in the cache. Fetching credentials from LakeFormation service.");
            try {
                // TODO: Implement proper Lake Formation integration
                // final GetTemporaryDataLocationCredentialsResponse response = getCredentialsFromLfService(
                //         lfClient, cacheKey);
                // final TemporaryCredentials temporaryCredentials = response.credentials();
                // credentials = AwsSessionCredentials.create(
                //     temporaryCredentials.accessKeyId(),
                //     temporaryCredentials.secretAccessKey(),
                //     temporaryCredentials.sessionToken()
                // );
                // final List<String> locations = response.accessibleDataLocations();
                // if (locations.isEmpty()) {
                //     throw new NoSuchElementException();
                // }
                // final String accessibleDataLocation = locations.get(0);
                // LOGGER.info("Caching the credentials for s3Prefix:" + accessibleDataLocation
                //     + " and permission: " + cacheKey.getPermission());
                // accessGrantsCache.put(
                //     new CacheKey(cacheKey, null, processMatchedTarget(accessibleDataLocation)),
                //     credentials
                // );
                // LOGGER.info("Successfully retrieved credentials from Lake Formation service.");

                // Temporary placeholder - will be implemented when Lake Formation classes are available
                throw new UnsupportedOperationException("Lake Formation integration pending - missing API classes");
            } catch (LakeFormationException e) {
                LOGGER.info("Exception occurred while fetching the credentials from Lake Formation: "
                    + e.getMessage());
                if ("AccessDenied".equals(e.awsErrorDetails().errorCode())) {
                    LOGGER.info("Caching the Access Denied request.");
                    accessDeniedCache.putValueInCache(cacheKey, e);
                }
                throw e;
            }
        }
        return credentials;
    }
}
