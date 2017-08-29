package com.zillow.zda.data_lake.credential;

import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import java.net.URI;
import java.util.Date;
import java.util.Map;

/**
 * An AWS credential provider that allows hadoop cluster to assume a role when read/write
 * from S3 buckets.
 * <p>
 * The credential provider extracts from the URI the AWS role ARN that it should assume.
 * To encode the role ARN, we use a custom URI query key-value pair. The key is defined as
 * a constant {@link RoleBasedAWSCredentialProvider#AWS_ROLE_ARN_KEY}, the value is encoded
 * AWS role ARN string.
 * If the ARN string does not present, then fallback to default AWS credentials provider
 * </p>
 */
public class RoleBasedAWSCredentialProvider implements AWSCredentialsProvider, Configurable {

    /**
     * URI query key for the ARN string
     */
    public final static String AWS_ROLE_ARN_KEY = "amz-assume-role-arn";

    /**
     * Time before expiry within which credentials will be renewed.
     */
    private static final int EXPIRY_TIME_MILLIS = 60 * 1000;

    /**
     * Life span of the temporary credential requested from STS
     */
    private static final int DURATION_TIME_SECONDS = 3600;

    /**
     * The original url containing a AWS role ARN in the query parameter.
     */
    private final URI uri;

    /**
     * AWS security token service instance.
     */
    private final AWSSecurityTokenService securityTokenService;

    /**
     * Abstraction of System time function for testing purpose
     */
    private final TimeProvider timeProvider;

    /**
     * The expiration time for the current session credentials.
     */
    private Date sessionCredentialsExpiration;

    /**
     * The current session credentials.
     */
    private AWSSessionCredentials sessionCredentials;

    /**
     * The arn of the role to be assumed.
     */
    private String roleArn;

    /**
     * Environment variable that may contain role to assume
     */
    private final String ROLE_ENV_VAR = "AWS_ROLE_ARN_KEY";
    
    /**
     * AWS authentication environment variables
     */
    private final String AWS_SECRET_KEY_ID = "AWS_SECRET_KEY_ID";
    private final String AWS_SECRET_ACCESS_KEY  = "AWS_SECRET_ACCESS_KEY";
    private final String AWS_SESSION_TOKEN  = "AWS_SESSION_TOKEN";

    private Logger logger = LogManager.getLogger(RoleBasedAWSCredentialProvider.class);



    /**
     * Create a {@link AWSCredentialsProvider} from an URI. The URI must contain a query parameter specifying
     * an AWS role ARN. The role is assumed to provide credentials for downstream operations.
     * <p>
     * The constructor signature must conform to hadoop calling convention exactly.
     * </p>
     *
     * @param uri           An URI containing role ARN parameter
     * @param configuration Hadoop configuration data
     */
    public RoleBasedAWSCredentialProvider(URI uri, Configuration configuration) {
        this.uri = uri;

        // This constructor is called by hadoop on EMR. The EMR instance must
        // have permission to access AWS security token service.
        // TODO - consider allow user to supply long-live credentials through hadoop configuration
        this.securityTokenService = new AWSSecurityTokenServiceClient();
        this.timeProvider = new TimeProvider() {
            @Override
            public long currentTimeMillis() {
                return System.currentTimeMillis();
            }
        };
        this.roleArn = configuration.get(AWS_ROLE_ARN_KEY);
        if(this.roleArn == null) {
            logger.warn("RoleBasedAWSCredentialProvider: No role provided via " + 
                        "Hadoop configuration. Checking environment variable " + this.ROLE_ENV_VAR + "...");
            
            Map<String, String> env = System.getenv();
            this.roleArn = env.get(this.ROLE_ENV_VAR);

            if (this.roleArn == null) {
                logger.warn("RoleBasedAWSCredentialProvider: Environment variable " + this.ROLE_ENV_VAR +
                            " not found. Not assuming a role.");
            } else {
                // This level is too high, but I want more visibility.
                // Eventually change to an info.
                logger.warn("RoleBasedAWSCredentialProvider: Using role ARN " + this.roleArn);
            }
        }
    }

    /**
     * Internal ctor for testing purpose
     *
     * @param uri                       An URI containing role ARN parameter
     * @param configuration             Hadoop configuration data
     * @param securityTokenService      AWS Security Token Service
     * @param timeProvider              Function interface to provide system time
     */
    RoleBasedAWSCredentialProvider(URI uri,
                                   Configuration configuration,
                                   AWSSecurityTokenService securityTokenService,
                                   TimeProvider timeProvider) {
        this.roleArn = configuration.get(AWS_ROLE_ARN_KEY);
        this.uri = uri;
        this.securityTokenService = securityTokenService;
        this.timeProvider = timeProvider;
    }


    @Override
    public AWSCredentials getCredentials() {
        this.logger.debug("get credential called");

        if(this.roleArn == null) {
            this.logger.warn("assume role not provided");
            return null;
        }

        if (needsNewSession()) {
            startSession();
        }
        return sessionCredentials;
    }

    @Override
    public void refresh() {
        logger.debug("refresh called");
        startSession();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }


    private AWSCredentials startSession() {
        try {
            String sessionName = "custom-credential-provider" + String.valueOf(this.timeProvider.currentTimeMillis());
            AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
                    .withRoleArn(this.roleArn)
                    .withRoleSessionName(sessionName)
                    .withDurationSeconds(DURATION_TIME_SECONDS);
            Credentials stsCredentials = securityTokenService.assumeRole(assumeRoleRequest).getCredentials();
            sessionCredentials = new BasicSessionCredentials(stsCredentials.getAccessKeyId(),
                    stsCredentials.getSecretAccessKey(), stsCredentials.getSessionToken());
            sessionCredentialsExpiration = stsCredentials.getExpiration();
        }
        catch (Exception ex) {
            logger.warn("Unable to start a new session. Will use old session credential or fallback credential", ex);
        }

        return sessionCredentials;
    }

    private boolean needsNewSession() {
        if (sessionCredentials == null) {
            // Increased log level from debug to warn
            logger.warn("Session credentials do not exist. Needs new session");
            return true;
        }

        long timeRemaining = sessionCredentialsExpiration.getTime() - timeProvider.currentTimeMillis();
        if(timeRemaining < EXPIRY_TIME_MILLIS) {
            // Increased log level from debug to warn
            logger.warn("Session credential exist but expired. Needs new session");
            return true;
        } else {
            // Increased log level from debug to warn
            logger.warn("Session credential exist and not expired. No need to create new session");
            return false;
        }
    }
}
