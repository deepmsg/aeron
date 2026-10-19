/*
 * A client that joins, proves its session is open, and then **vanishes**.
 *
 * The reference's session-timeout cases work the same way: the client stops
 * without sending a close-session request, and the consensus module closes the
 * session when it has been idle for `aeron.cluster.session.timeout`. A normal
 * `AeronCluster.close()` would be a `CLIENT_ACTION` close and would test the
 * wrong path, so this one halts the JVM (`Runtime.halt`) — no shutdown hook, no
 * close request, nothing on the wire.
 *
 * Receiving the echo before halting is what makes the session's existence a
 * fact rather than an assumption: the business has already answered it, so the
 * session is open in the log.
 *
 * Usage: <aeron dir> [timeoutSeconds]
 *
 *     ABANDONED         the session is open and the process is about to halt
 *     OFFER_FAILED <r>  the cluster never accepted a message
 *     NO_ECHO           the answer never came, so the session is unproven
 *     CONNECT_FAILED    the client never joined
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterAbandonSessionProbe.java
 */

import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterAbandonSessionProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final long timeoutSeconds = args.length > 1 ? Long.parseLong(args[1]) : 20;
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
            final UnsafeBuffer message = new UnsafeBuffer(new byte[4]);
            final long deadline = System.nanoTime() + timeoutNs;

            long result;
            while ((result = cluster.offer(message, 0, message.capacity())) < 0)
            {
                if (System.nanoTime() > deadline)
                {
                    System.out.println("OFFER_FAILED " + result);
                    Runtime.getRuntime().halt(1);
                }
                cluster.pollEgress();
                Thread.yield();
            }

            while (received.get() < 1 && System.nanoTime() < deadline)
            {
                cluster.pollEgress();
                Thread.yield();
            }

            if (received.get() < 1)
            {
                System.out.println("NO_ECHO");
                Runtime.getRuntime().halt(1);
            }

            System.out.println("ABANDONED");
            System.out.flush();
            Runtime.getRuntime().halt(0);
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
        }
    }
}
