/*
 * The Java side of LC-1's challenge/response case: a cluster authenticator that
 * makes every client answer a question before its session opens.
 *
 * The cluster loads an authenticator **by class name** from a property
 * (`aeron.cluster.authenticator.supplier`, `ConsensusModule.java:807`), and the
 * reference's `aeron-all` jar carries only `DefaultAuthenticatorSupplier`
 * (accepts everyone) and `SimpleAuthenticator` (credentials on the connect
 * request, no challenge). This is the missing shape, and it is a probe rather
 * than product code because nothing in a deployment needs this particular
 * challenge — what it exists for is our client's `CredentialsSupplier` path.
 *
 * The protocol it speaks, on one line each:
 *
 *   onConnectRequest       the client sent no credentials; remember the session
 *   onConnectedSession     ask the client to answer `lc1-challenge`
 *   onChallengeResponse    remember whether the answer was `lc1-answer`
 *   onChallengedSession    authenticate or reject, and print `CHALLENGE_OK` on
 *                          the way so the test can see this class ran at all
 *
 * The print goes to the node's own log, which is what the fixture reads
 * (`TestCluster::node_log`).
 *
 * Usage: named by `-Daeron.cluster.authenticator.supplier=ChallengeAuthenticatorSupplier`
 * on a `ClusteredMediaDriver` node whose classpath also carries this class.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ChallengeAuthenticatorSupplier.java
 */

import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.SessionProxy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ChallengeAuthenticatorSupplier implements AuthenticatorSupplier
{
    private static final byte[] CHALLENGE = "lc1-challenge".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ANSWER = "lc1-answer".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PRINCIPAL = "lc1-user".getBytes(StandardCharsets.US_ASCII);

    @Override
    public Authenticator get()
    {
        return new ChallengeAuthenticator();
    }

    private static final class ChallengeAuthenticator implements Authenticator
    {
        /// No answer yet, or the answer that came: the session proxy is called
        /// repeatedly, and a session with no answer must be left alone rather
        /// than rejected before its client has had the chance to reply.
        private final Map<Long, Boolean> answered = new HashMap<>();

        @Override
        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
            // The client sends nothing on the connect request: the challenge is
            // the whole of what this authenticator asks. Nothing to record.
        }

        @Override
        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
            final boolean correct = java.util.Arrays.equals(ANSWER, encodedCredentials);
            answered.put(sessionId, correct);
            System.out.println("CHALLENGE_RESPONSE sessionId=" + sessionId + " correct=" + correct);
        }

        @Override
        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            if (answered.containsKey(sessionProxy.sessionId()))
            {
                return;
            }

            if (sessionProxy.challenge(CHALLENGE))
            {
                System.out.println("CHALLENGE_SENT sessionId=" + sessionProxy.sessionId());
            }
        }

        @Override
        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            final long sessionId = sessionProxy.sessionId();
            final Boolean correct = answered.get(sessionId);

            if (Boolean.TRUE.equals(correct))
            {
                if (sessionProxy.authenticate(PRINCIPAL))
                {
                    System.out.println("CHALLENGE_OK sessionId=" + sessionId);
                }
            }
            else if (Boolean.FALSE.equals(correct))
            {
                System.out.println("CHALLENGE_REJECTED sessionId=" + sessionId);
                sessionProxy.reject();
            }
            // No answer yet: the client is still being asked, and rejecting
            // here would be rejecting every session the first time this is
            // called.
        }
    }
}
