/*
 * C4e: an `AeronCluster` client that sends one admin request and prints the
 * response, so the CM's `onAdminRequest` can be judged on the wire.
 *
 * Usage: <aeron dir> [waitMillis] [SNAPSHOT|NULL_VAL]
 *
 * Prints:
 *     CONNECTED <epochMillis>
 *     SESSION_EVENT <epochMillis> <code> <detail>
 *     ADMIN_RESPONSE <code> <message>
 *     RESPONSE_SEEN | NO_RESPONSE
 *     CONNECT_FAILED <exception>
 *
 * `SNAPSHOT` uses `AeronCluster.sendAdminRequestToTakeASnapshot`; `NULL_VAL`
 * encodes the raw `AdminRequest` the way the reference's
 * `ClusterTest.shouldRejectAnInvalidAdminRequest` does, because the client API
 * only exposes the snapshot request.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterAdminRequestProbe.java
 */

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.AdminRequestEncoder;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.AdminResponseCode;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterAdminRequestProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final long waitMillis = args.length > 1 ? Long.parseLong(args[1]) : 10000;
        final String requestName = args.length > 2 ? args[2] : "SNAPSHOT";

        final long correlationId = System.nanoTime();
        final AtomicBoolean responded = new AtomicBoolean();

        final EgressListener listener = new EgressListener()
        {
            public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
            }

            public void onAdminResponse(
                final long clusterSessionId,
                final long responseCorrelationId,
                final AdminRequestType requestType,
                final AdminResponseCode responseCode,
                final String message,
                final DirectBuffer payload,
                final int payloadOffset,
                final int payloadLength)
            {
                if (responseCorrelationId == correlationId)
                {
                    System.out.println("ADMIN_RESPONSE " + responseCode + " " + message);
                    System.out.flush();
                    responded.set(true);
                }
            }

            public void onSessionEvent(
                final long eventCorrelationId,
                final long clusterSessionId,
                final long leadershipTermId,
                final int leaderMemberId,
                final EventCode code,
                final String detail)
            {
                System.out.println("SESSION_EVENT " + System.currentTimeMillis() + " " + code + " " + detail);
                System.out.flush();
            }
        };

        final AeronCluster.Context context = new AeronCluster.Context()
            .aeronDirectoryName(aeronDir)
            .ingressChannel("aeron:ipc")
            .ingressStreamId(INGRESS_STREAM_ID)
            .egressChannel("aeron:ipc")
            .egressStreamId(EGRESS_STREAM_ID)
            .egressListener(listener);

        try (AeronCluster cluster = AeronCluster.connect(context))
        {
            System.out.println("CONNECTED " + System.currentTimeMillis());
            System.out.flush();

            if ("SNAPSHOT".equals(requestName))
            {
                while (!cluster.sendAdminRequestToTakeASnapshot(correlationId) && !responded.get())
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }
            else
            {
                final AdminRequestEncoder encoder = new AdminRequestEncoder();
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
                encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .leadershipTermId(cluster.leadershipTermId())
                    .clusterSessionId(cluster.clusterSessionId())
                    .correlationId(correlationId)
                    .requestType(AdminRequestType.NULL_VAL);

                final Publication publication = cluster.ingressPublication();
                final int length = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
                while (publication.offer(encoder.buffer(), 0, length) < 0 && !responded.get())
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }

            final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis);
            while (!responded.get() && System.nanoTime() < deadline)
            {
                cluster.pollEgress();
                Thread.yield();
            }

            System.out.println(responded.get() ? "RESPONSE_SEEN" : "NO_RESPONSE");
            System.out.flush();
        }
        catch (final Exception ex)
        {
            System.out.println("CONNECT_FAILED " + ex);
            System.out.flush();
        }
    }
}
