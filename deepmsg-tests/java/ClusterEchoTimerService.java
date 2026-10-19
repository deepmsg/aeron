/*
 * The Java twin of `deepmsg_cluster_service::services::echo`, for the "all
 * Java" arm of every LC-4a scenario.
 *
 * The point of that arm is to prove the **fixture** reads a scenario the way
 * the reference does: the same client probe, the same container launch path,
 * the same assertions — only the business is the reference's. So this business
 * must answer the same messages the Rust one does:
 *
 *   * any message: echoed to the session that sent it;
 *   * `Echo as Service IPC ingress`: offered back into the cluster, and the
 *     service message that returns echoed to every session
 *     (TestNode.java:669-690);
 *   * 16 bytes: `cmd: i32 @0` (1 schedule / 2 cancel), `correlationId: i64 @4`,
 *     `delayMillis: i32 @12`; a fired timer echoes its 8-byte correlation id
 *     to the session that scheduled it.
 *
 * The timer command shape is this build's, not the reference's, so the twin is
 * the only Java thing that speaks it.
 *
 * Usage (as a class name in `ClusterServiceContainerProbe`): this class, with
 * a public no-argument constructor.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterEchoTimerService.java
 */

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ClusterEchoTimerService implements ClusteredService
{
    private static final String ECHO_SERVICE_IPC_INGRESS_MSG = "Echo as Service IPC ingress";
    private static final int TIMER_SCHEDULE = 1;
    private static final int TIMER_CANCEL = 2;
    private static final int TIMER_COMMAND_LENGTH = 16;

    private final List<long[]> pending = new ArrayList<>();
    private Cluster cluster;
    private IdleStrategy idleStrategy;

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (length == ECHO_SERVICE_IPC_INGRESS_MSG.length() &&
            ECHO_SERVICE_IPC_INGRESS_MSG.equals(buffer.getStringWithoutLengthAscii(offset, length)))
        {
            sendServiceIpcMessage(session, buffer, offset, length);
            return;
        }

        if (TIMER_COMMAND_LENGTH == length)
        {
            final int command = buffer.getInt(offset, java.nio.ByteOrder.LITTLE_ENDIAN);
            final long correlationId = buffer.getLong(offset + 4, java.nio.ByteOrder.LITTLE_ENDIAN);
            final int delayMillis = buffer.getInt(offset + 12, java.nio.ByteOrder.LITTLE_ENDIAN);

            if (TIMER_SCHEDULE == command)
            {
                final long deadline = timestamp + delayMillis;
                if (cluster.scheduleTimer(correlationId, deadline))
                {
                    pending.add(new long[]{ correlationId, null == session ? -1 : session.id() });
                }
            }
            else if (TIMER_CANCEL == command)
            {
                cluster.cancelTimer(correlationId);
            }

            return;
        }

        if (null != session)
        {
            echoMessage(session, buffer, offset, length);
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        final Iterator<long[]> iterator = pending.iterator();
        while (iterator.hasNext())
        {
            final long[] entry = iterator.next();
            if (entry[0] == correlationId)
            {
                iterator.remove();
                if (-1 != entry[1])
                {
                    final ClientSession session = findSession(entry[1]);
                    if (null != session)
                    {
                        final byte[] payload = new byte[8];
                        java.nio.ByteBuffer.wrap(payload)
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                            .putLong(correlationId);
                        echoMessage(session, new UnsafeBuffer(payload), 0, 8);
                    }
                }
                return;
            }
        }
    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    public void onTerminate(final Cluster cluster)
    {
    }

    private ClientSession findSession(final long id)
    {
        for (final ClientSession session : cluster.clientSessions())
        {
            if (session.id() == id)
            {
                return session;
            }
        }
        return null;
    }

    private void sendServiceIpcMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (null != session)
        {
            idleStrategy.reset();
            while (cluster.offer(buffer, offset, length) < 0)
            {
                idleStrategy.idle();
            }
        }
        else
        {
            for (final ClientSession clientSession : cluster.clientSessions())
            {
                echoMessage(clientSession, buffer, offset, length);
            }
        }
    }

    private void echoMessage(
        final ClientSession session, final DirectBuffer buffer, final int offset, final int length)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = session.offer(buffer, offset, length);
            if (result > 0)
            {
                return;
            }

            if (Publication.BACK_PRESSURED == result)
            {
                idleStrategy.idle();
            }
            else if (Publication.ADMIN_ACTION != result)
            {
                throw new IllegalStateException("egress publication error: " +
                    Publication.errorString(result) + ", clusterSessionId=" + session.id());
            }
        }
    }
}
