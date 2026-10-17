/*
 * The service half of LC-4b: a Java container against deepmsg's consensus
 * module.
 *
 * It is the smallest service the reference's own `ClusteredService` interface
 * accepts: whatever a client offers on its session goes straight back to that
 * session. That is enough to exercise the whole S2 seam — the CM's `JoinLog`,
 * the log recording and replay that carries the client's message to the
 * container, the container's egress back through the CM, and the client's
 * delivery — none of which our own fake service can falsify, because it shares
 * code with the CM.
 *
 * It is loaded by `ClusteredServiceContainer` through
 * `aeron.cluster.service.class.name` and reflection (a public class with a
 * public no-argument constructor), which is why it has no package and no
 * constructor of its own.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterEchoService.java
 */

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

public class ClusterEchoService implements ClusteredService
{
    private static final int SEND_ATTEMPTS = 3;

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
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = session.offer(buffer, offset, length);
            if (result > 0)
            {
                return;
            }
            idleStrategy.idle();
        }
        while (--attempts > 0);
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {
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
}
