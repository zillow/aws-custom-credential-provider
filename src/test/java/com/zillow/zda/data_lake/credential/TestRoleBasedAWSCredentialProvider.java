package com.zillow.zda.data_lake.credential;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;

import java.net.URI;
import java.util.Calendar;
import java.util.Date;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * Created by weil1 on 10/31/16.
 */
public class TestRoleBasedAWSCredentialProvider {

    @Test
    public void shouldReturnNullIfNoAssumeRoleProvided() {
        URI uri = URI.create("https://user@s3.amazonaws.com/catalog.zillow.net/tes100/rss/" +
                "property/deleted_AL.xml");
        AWSSecurityTokenService tokenService = mock(AWSSecurityTokenService.class);

        // Mock 3 consecutive token service calls that returns 3 credential tokens
        // Each expires 1 hour after the previous one.
        Calendar calendar = Calendar.getInstance();
        calendar.set(2000, 1, 2, 0, 0, 0);
        AssumeRoleResult mockResult1 = CreateMockResponse(calendar, "session1");
        when(tokenService.assumeRole(isA(AssumeRoleRequest.class))).thenReturn(mockResult1);

        // The first call to the service should return a session based credential with matching
        // session id, key id and key.
        calendar.set(2000, 1, 1, 23, 50, 0);
        Date callDate1 = calendar.getTime();
        EchoTimeProvider timeProvider = new EchoTimeProvider(callDate1);

        Configuration configuration = new Configuration();
        AWSCredentialsProvider provider = new RoleBasedAWSCredentialProvider(uri, configuration, tokenService, timeProvider);
        Assert.assertEquals(null, provider.getCredentials());
    }

    @Test
    public void shouldMaintainAndExpireSessionCredential() {
        URI uri = URI.create("https://user@s3.amazonaws.com/catalog.zillow.net/tes100/rss/" +
                "property/deleted_AL.xml");
        AWSSecurityTokenService tokenService = mock(AWSSecurityTokenService.class);

        // Mock 3 consecutive token service calls that returns 3 credential tokens
        // Each expires 1 hour after the previous one.
        Calendar calendar = Calendar.getInstance();
        calendar.set(2000, 1, 2, 0, 0, 0);
        AssumeRoleResult mockResult1 = CreateMockResponse(calendar, "session1");
        calendar.set(2000, 1, 2, 1, 0, 0);
        AssumeRoleResult mockResult2 = CreateMockResponse(calendar, "session2");
        calendar.set(2000, 1, 2, 3, 0, 0);
        AssumeRoleResult mockResult3 = CreateMockResponse(calendar, "session3");

        when(tokenService.assumeRole(isA(AssumeRoleRequest.class))).thenReturn(mockResult1, mockResult2, mockResult3);

        // The first call to the service should return a session based credential with matching
        // session id, key id and key.
        calendar.set(2000, 1, 1, 23, 50, 0);
        Date callDate1 = calendar.getTime();
        EchoTimeProvider timeProvider = new EchoTimeProvider(callDate1);

        Configuration configuration = new Configuration();
        configuration.set(RoleBasedAWSCredentialProvider.AWS_ROLE_ARN_KEY, "role1");
        AWSCredentialsProvider provider = new RoleBasedAWSCredentialProvider(uri, configuration, tokenService, timeProvider);
        ValidateSessionId(provider, "session1");

        // 10 seconds passes, session 1 should still be valid.
        calendar.set(2000, 1, 1, 23, 50, 10);
        Date callDate2 = calendar.getTime();
        timeProvider.setDate(callDate2);
        ValidateSessionId(provider, "session1");

        // The first session is almost expiring
        calendar.set(2000, 1, 31, 23, 59, 10);
        Date callDate3 = calendar.getTime();
        timeProvider.setDate(callDate3);
        ValidateSessionId(provider, "session2");

        // Calling refresh would force refresh a new session even if the time hasn't expire yet.
        provider.refresh();
        ValidateSessionId(provider, "session3");
    }

    private AssumeRoleResult CreateMockResponse(Calendar calendar, String sessionId) {
        Date expireDate1 = calendar.getTime();
        Credentials credential1 = new Credentials("keyid", "key", sessionId, expireDate1);
        return new AssumeRoleResult().withCredentials(credential1);
    }

    private void ValidateSessionId(AWSCredentialsProvider provider, String sessionId) {
        AWSCredentials producedCredentials = provider.getCredentials();
        Assert.assertTrue(producedCredentials instanceof BasicSessionCredentials);
        BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) producedCredentials;
        Assert.assertEquals("keyid", sessionCredentials.getAWSAccessKeyId());
        Assert.assertEquals("key", sessionCredentials.getAWSSecretKey());
        Assert.assertEquals(sessionId, sessionCredentials.getSessionToken());
    }

    /**
     * A time provider that can be changed in runtime for testing purpose.
     */
    private class EchoTimeProvider implements TimeProvider {

        private Date date;

        public EchoTimeProvider(Date date) {
            this.date = date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        @Override
        public long currentTimeMillis() {
            return this.date.getTime();
        }
    }
}
