/*
 * A client that stays connected and reports what the cluster tells its
 * session while it is open -- the probe the C4d control/termination cases need.
 *
 * Unlike `ClusterEchoClientProbe` it does not stop at the echo: it offers
 * `count` messages, keeps polling egress (and sends a keep-alive every second)
 * for `waitMillis`, and prints:
 *
 *     ECHO <epochMillis> <index>        one line per message echoed back
 *     OFFERED <epochMillis> <index> <position>
 *     SESSION_EVENT <epochMillis> <code> <detail>
 *     IMAGES <epochMillis> <n>          every change of the egress image count
 *     PROGRESS <epochMillis> <received> <images> every 500 ms, so a caller can
 *                                       tell "nothing arrived before the RESUME"
 *                                       from "nothing arrived at all"
 *     RECEIVED <n>
 *     FINAL_IMAGES <n>                  the last egress image count seen
 *     SESSION_CLOSED | NO_SESSION_CLOSE
 *     CONNECTED <epochMillis>           right after the session is open
 *     OFFER_FAILED <result>             the cluster never took a message
 *     CONNECT_FAILED <exception>
 *
 * Usage: <aeron dir> <waitMillis> [count] [sendDelayMillis]
 *
 * `sendDelayMillis` lets a case take the cluster away (SUSPEND) after the
 * session is open but before the first offer, so the messages are offered
 * while the module is not polling ingress.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterSessionEventProbe.java
 */

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterSessionEventProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final long waitMillis = args.length > 1 ? Long.parseLong(args[1]) : 20000;
        final int count = args.length > 2 ? Integer.parseInt(args[2]) : 1;
        final long sendDelayMillis = args.length > 3 ? Long.parseLong(args[3]) : 0;
        final AtomicInteger received = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();

        final AeronCluster.Context context = new AeronCluster.Context()
            .aeronDirectoryName(aeronDir)
            .ingressChannel("aeron:ipc")
            .ingressStreamId(INGRESS_STREAM_ID)
            .egressChannel("aeron:ipc")
            .egressStreamId(EGRESS_STREAM_ID)
            .egressListener(new EgressListener()
            {
                public void onMessage(
                    final long clusterSessionId,
                    final long timestamp,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    final int index = received.getAndIncrement();
                    System.out.println("ECHO " + System.currentTimeMillis() + " " + index);
                    System.out.flush();
                }

                public void onSessionEvent(
                    final long correlationId,
                    final long clusterSessionId,
                    final long leadershipTermId,
                    final int leaderMemberId,
                    final EventCode code,
                    final String detail)
                {
                    System.out.println(
                        "SESSION_EVENT " + System.currentTimeMillis() + " " + code + " " + detail);
                    System.out.flush();
                    if (EventCode.CLOSED == code)
                    {
                        closed.set(true);
                    }
                }
            });

        long result = -1;
        long images = -1;
        try (AeronCluster cluster = AeronCluster.connect(context))
        {
            System.out.println("CONNECTED " + System.currentTimeMillis());
            System.out.flush();

            final UnsafeBuffer message = new UnsafeBuffer(new byte[4]);
            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);

            final long sendAt =
                System.currentTimeMillis() + sendDelayMillis;
            while (System.currentTimeMillis() < sendAt && !closed.get())
            {
                cluster.pollEgress();
                Thread.yield();
            }

            for (int i = 0; i < count && !closed.get() && System.nanoTime() < deadline; i++)
            {
                message.putInt(0, i, java.nio.ByteOrder.LITTLE_ENDIAN);
                result = cluster.offer(message, 0, message.capacity());
                while (result < 0 && !closed.get() && System.nanoTime() < deadline)
                {
                    cluster.pollEgress();
                    Thread.yield();
                    result = cluster.offer(message, 0, message.capacity());
                }
                if (result < 0)
                {
                    System.out.println("OFFER_FAILED " + result);
                    System.out.flush();
                    break;
                }
                System.out.println("OFFERED " + System.currentTimeMillis() + " " + i + " " + result);
                System.out.flush();
            }

            long lastProgress = 0;
            long lastKeepAlive = 0;
            while (!closed.get() && System.nanoTime() < deadline)
            {
                cluster.pollEgress();

                final long imageCount = cluster.egressSubscription().imageCount();
                if (imageCount != images)
                {
                    images = imageCount;
                    System.out.println("IMAGES " + System.currentTimeMillis() + " " + images);
                    System.out.flush();
                }

                final long nowMillis = System.currentTimeMillis();
                if (nowMillis - lastProgress >= 500)
                {
                    lastProgress = nowMillis;
                    System.out.println("PROGRESS " + nowMillis + " " + received.get() + " " + images);
                    System.out.flush();
                }
                if (nowMillis - lastKeepAlive >= 1000)
                {
                    lastKeepAlive = nowMillis;
                    cluster.sendKeepAlive();
                }
                Thread.yield();
            }
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
        }

        System.out.println("RECEIVED " + received.get());
        System.out.println("FINAL_IMAGES " + images);
        System.out.println(closed.get() ? "SESSION_CLOSED" : "NO_SESSION_CLOSE");
        System.out.flush();
    }
}
