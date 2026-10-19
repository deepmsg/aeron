/*
 * The client half of LC-4b: the reference's own `AeronCluster` against
 * deepmsg's consensus module and the reference's own service container.
 *
 * The client connects, offers `count` messages, and waits for one echo per
 * message. It prints one reading and exits 0 whatever the count turned out to
 * be — the test is what decides whether the reading is a pass:
 *
 *     RECEIVED <n>
 *
 * `OFFER_FAILED` is printed instead if the cluster never accepted a message.
 *
 * Usage: <aeron dir> <message count> [timeoutSeconds] [expectedAnswers]
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterEchoClientProbe.java
 */

import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterEchoClientProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;
    private static final long DEFAULT_TIMEOUT_SECONDS = 20;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final int count = Integer.parseInt(args[1]);
        final long timeoutSeconds = args.length > 2 ?
            Long.parseLong(args[2]) : DEFAULT_TIMEOUT_SECONDS;
        final long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        // A multi-service cluster answers each message once per service, so the
        // final reading can be more than the message count. Without this the
        // probe would print as soon as the *first* answer of the last message
        // arrived, and a lost duplicate would look like a smaller number rather
        // than a failure.
        final int expectedAnswers = args.length > 3 ? Integer.parseInt(args[3]) : count;
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
            final UnsafeBuffer message = new UnsafeBuffer(new byte[4]);
            final long deadline = System.nanoTime() + timeoutNs;

            for (int i = 0; i < count; i++)
            {
                message.putInt(0, i, java.nio.ByteOrder.LITTLE_ENDIAN);

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

                while (received.get() <= i && System.nanoTime() < deadline)
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }

            while (received.get() < expectedAnswers && System.nanoTime() < deadline)
            {
                cluster.pollEgress();
                Thread.yield();
            }

            System.out.println("RECEIVED " + received.get());
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
        }
    }
}
