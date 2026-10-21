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
 *        [ingressChannel] [ingressEndpoints] [egressChannel] [pauseMillis]
 *
 * The last three are LC-2's: a client of a cluster whose members are **not**
 * in this JVM cannot reach it over IPC, so the caller names the cluster's
 * ingress channel (no endpoint), the `id=endpoint` list its members advertise
 * and an egress channel to be answered on. With none of them the probe is what
 * it always was — the LC-4b shape, IPC against a node in the same driver.
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
        final String ingressChannel = args.length > 4 ? args[4] : "aeron:ipc";
        final String ingressEndpoints = args.length > 5 && !args[5].isEmpty() ? args[5] : null;
        final String egressChannel = args.length > 6 ? args[6] : "aeron:ipc";
        // A message rate, when a case has to keep the client busy across a
        // window the cluster is made to lose (P3-6c's partition cases): without
        // it a fast cluster finishes the whole count in a second and the window
        // lands after the run.
        final long pauseMillis = args.length > 7 ? Long.parseLong(args[7]) : 0;
        final AtomicInteger received = new AtomicInteger();

        final AeronCluster.Context context = new AeronCluster.Context()
            .aeronDirectoryName(aeronDir)
            .ingressChannel(ingressChannel)
            .ingressStreamId(INGRESS_STREAM_ID)
            .egressChannel(egressChannel)
            .egressStreamId(EGRESS_STREAM_ID)
            .egressListener((sessionId, timestamp, buffer, offset, length, header) ->
                received.incrementAndGet());

        if (null != ingressEndpoints)
        {
            context.ingressEndpoints(ingressEndpoints);
        }

        try (AeronCluster cluster = AeronCluster.connect(context))
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

                if (pauseMillis > 0 && i + 1 < count)
                {
                    // A pause is a *live* wait, not an absent client: the
                    // reference's `AeronCluster` closes itself when a new-leader
                    // event is not followed by a successful send within
                    // `messageTimeoutNs` (`AeronCluster.pollStateChanges`,
                    // `AeronCluster.java:646-660`, and `trackIngressPublicationResult`,
                    // `:713-734`), and a case that pauses across a leadership
                    // change must keep the connection alive the way
                    // `sendKeepAlive` is for (`:487-520`).
                    final long pauseUntil = System.currentTimeMillis() + pauseMillis;
                    long lastKeepAlive = 0;
                    while (System.currentTimeMillis() < pauseUntil && System.nanoTime() < deadline)
                    {
                        cluster.pollEgress();

                        if (System.currentTimeMillis() - lastKeepAlive >= 500)
                        {
                            lastKeepAlive = System.currentTimeMillis();
                            cluster.sendKeepAlive();
                        }

                        Thread.sleep(1);
                    }
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
