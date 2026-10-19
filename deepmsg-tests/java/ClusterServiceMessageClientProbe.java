/*
 * The client half of the service-message scenario: the reference's own
 * `AeronCluster` sends `Echo as Service IPC ingress` and counts what comes
 * back.
 *
 * That message is the one `TestNode.TestService` treats differently from an
 * ordinary echo (`TestNode.java:669-690`): the service offers it back into the
 * cluster, the log carries it to every service, and the service message that
 * comes out has no client session — so the service echoes it to **every**
 * session. A client that gets exactly one answer per message has been through
 * that whole round trip once, with no duplicate and no loss.
 *
 * Usage: <aeron dir> [messageCount] [timeoutSeconds]
 *
 *     RECEIVED <n>          answers seen
 *     OFFER_FAILED <code>   the cluster never accepted a message
 *     CONNECT_FAILED <ex>   the client never joined
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterServiceMessageClientProbe.java
 */

import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterServiceMessageClientProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;
    private static final byte[] SERVICE_MESSAGE =
        "Echo as Service IPC ingress".getBytes(StandardCharsets.US_ASCII);

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final int messageCount = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        final long timeoutSeconds = args.length > 2 ? Long.parseLong(args[2]) : 30;
        final long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        final AtomicInteger received = new AtomicInteger();

        try (AeronCluster cluster = AeronCluster.connect(
            new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:ipc")
                .ingressStreamId(INGRESS_STREAM_ID)
                .egressChannel("aeron:ipc")
                .egressStreamId(EGRESS_STREAM_ID)
                .egressListener((sessionId, timestamp, buffer, offset, length, header) ->
                    received.incrementAndGet())))
        {
            final UnsafeBuffer message = new UnsafeBuffer(SERVICE_MESSAGE);
            final long deadline = System.nanoTime() + timeoutNs;

            for (int i = 0; i < messageCount; i++)
            {
                long result;
                while ((result = cluster.offer(message, 0, message.capacity())) < 0)
                {
                    if (System.nanoTime() > deadline)
                    {
                        System.out.println("OFFER_FAILED " + result);
                        return;
                    }
                    cluster.pollEgress();
                    Thread.yield();
                }

                // Wait for this message's answer before sending the next, so a
                // duplicate is a count that runs ahead rather than a race.
                while (received.get() <= i && System.nanoTime() < deadline)
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }

            System.out.println("RECEIVED " + received.get());
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
        }
    }
}
