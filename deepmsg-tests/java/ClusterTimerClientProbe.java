/*
 * The timer half of LC-4a's smoke: the reference's own `AeronCluster` sends
 * schedule/cancel commands and prints what arrives back.
 *
 * The commands are 16 bytes, the shape deepmsg's `EchoService` test support
 * decodes:
 *
 *     int32  command     (1 = schedule, 2 = cancel)
 *     int64  correlationId
 *     int32  delayMillis (schedule only; cluster time is milliseconds)
 *
 * A service that has fired a timer answers with the 8-byte correlation id.
 * The probe is its own oracle and prints:
 *
 *     TIMER <id>            a timer event arrived
 *     CANCELLED <id>        the id was scheduled, cancelled, and did not arrive
 *     MISSING <id>          a timer that should have arrived did not
 *     OFFER_FAILED <result> the cluster never accepted a command
 *     CONNECT_FAILED <ex>   the client never joined
 *
 * Usage: <aeron dir> [timeoutSeconds]
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterTimerClientProbe.java
 */

import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterTimerClientProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;

    private static final int COMMAND_LENGTH = 16;
    private static final int TIMER_LENGTH = 8;
    private static final int SCHEDULE = 1;
    private static final int CANCEL = 2;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final long timeoutSeconds = args.length > 1 ? Long.parseLong(args[1]) : 30;
        final long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        final ConcurrentHashMap<Long, Long> timers = new ConcurrentHashMap<>();
        final AtomicLong arrivals = new AtomicLong();

        try (AeronCluster cluster = AeronCluster.connect(
            new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:ipc")
                .ingressStreamId(INGRESS_STREAM_ID)
                .egressChannel("aeron:ipc")
                .egressStreamId(EGRESS_STREAM_ID)
                .egressListener((sessionId, timestamp, buffer, offset, length, header) ->
                {
                    if (TIMER_LENGTH == length)
                    {
                        final long correlationId = buffer.getLong(offset);
                        timers.put(correlationId, timestamp);
                        System.out.println("TIMER " + correlationId);
                    }
                    arrivals.incrementAndGet();
                })))
        {
            final long deadline = System.nanoTime() + timeoutNs;

            if (!offer(cluster, SCHEDULE, 1L, 300))
            {
                System.out.println("OFFER_FAILED schedule 1");
                return;
            }
            if (!awaitTimer(cluster, timers, 1L, deadline))
            {
                System.out.println("MISSING 1");
                return;
            }

            // Scheduled and cancelled immediately: the delay is long enough that
            // a cancel that arrived would have removed it, and the wait is long
            // enough that a cancel that did nothing would have fired it.
            if (!offer(cluster, SCHEDULE, 2L, 300) || !offer(cluster, CANCEL, 2L, 0))
            {
                System.out.println("OFFER_FAILED schedule/cancel 2");
                return;
            }
            drainFor(cluster, TimeUnit.MILLISECONDS.toNanos(1200));
            if (timers.containsKey(2L))
            {
                System.out.println("MISSING_CANCEL 2");
                return;
            }
            System.out.println("CANCELLED 2");

            if (!offer(cluster, SCHEDULE, 3L, 100))
            {
                System.out.println("OFFER_FAILED schedule 3");
                return;
            }
            if (!awaitTimer(cluster, timers, 3L, deadline))
            {
                System.out.println("MISSING 3");
            }
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
        }
    }

    private static boolean offer(
        final AeronCluster cluster,
        final int command,
        final long correlationId,
        final int delayMillis)
    {
        final ByteBuffer bytes = ByteBuffer.allocate(COMMAND_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        bytes.putInt(command);
        bytes.putLong(correlationId);
        bytes.putInt(delayMillis);
        final UnsafeBuffer message = new UnsafeBuffer(bytes.array());
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (true)
        {
            final long result = cluster.offer(message, 0, COMMAND_LENGTH);
            if (result > 0)
            {
                return true;
            }
            if (System.nanoTime() > deadline)
            {
                return false;
            }
            cluster.pollEgress();
            Thread.yield();
        }
    }

    private static boolean awaitTimer(
        final AeronCluster cluster,
        final ConcurrentHashMap<Long, Long> timers,
        final long correlationId,
        final long deadline)
    {
        while (System.nanoTime() < deadline)
        {
            if (timers.containsKey(correlationId))
            {
                return true;
            }
            cluster.pollEgress();
            Thread.yield();
        }

        return timers.containsKey(correlationId);
    }

    private static void drainFor(final AeronCluster cluster, final long durationNs)
    {
        final long deadline = System.nanoTime() + durationNs;
        while (System.nanoTime() < deadline)
        {
            cluster.pollEgress();
            Thread.yield();
        }
    }
}
