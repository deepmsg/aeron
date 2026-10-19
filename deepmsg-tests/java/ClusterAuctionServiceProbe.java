/*
 * The Java half of P3-2b's twin comparison: the reference's own
 * `ClusteredServiceContainer` running the tutorial's auction, beside the Rust
 * container running `deepmsg_cluster_service::services::BasicAuction`, on the
 * same consensus module and the same log.
 *
 * The business is a verbatim copy of
 * `aeron-samples/.../cluster/tutorial/BasicAuctionClusteredService`, minus its
 * `System.out.println` calls (tutorial output, not behaviour). Byte equality of
 * the two snapshots depends on this file and the Rust twin staying identical in
 * three places: the 24-byte bid, the 25-byte egress and the 16-byte snapshot
 * record.
 *
 * Usage: <aeron dir> <cluster dir> <cluster id> <service id>
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterAuctionServiceProbe.java
 */

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.collections.MutableBoolean;

public class ClusterAuctionServiceProbe
{
    public static void main(final String[] args) throws Exception
    {
        final String aeronDir = args[0];
        final String clusterDir = args[1];
        final int clusterId = Integer.parseInt(args[2]);
        final int serviceId = Integer.parseInt(args[3]);

        final ClusteredServiceContainer.Context context = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDirectoryName(clusterDir)
            .clusterId(clusterId)
            .serviceId(serviceId)
            .controlChannel("aeron:ipc?term-length=128k")
            .clusteredService(new BasicAuctionClusteredService())
            .isRespondingService(0 == serviceId);

        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(context);
        Runtime.getRuntime().addShutdownHook(new Thread(container::close));
        System.out.println("STARTED " + serviceId);

        Thread.currentThread().join();
    }

    /**
     * The reference's tutorial business
     * (`BasicAuctionClusteredService.java:44-323`), without its prints.
     */
    static class BasicAuctionClusteredService implements ClusteredService
    {
        static final int CORRELATION_ID_OFFSET = 0;
        static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + 8;
        static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + 8;
        static final int BID_MESSAGE_LENGTH = PRICE_OFFSET + 8;
        static final int BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
        static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + 1;

        static final int SNAPSHOT_CUSTOMER_ID_OFFSET = 0;
        static final int SNAPSHOT_PRICE_OFFSET = SNAPSHOT_CUSTOMER_ID_OFFSET + 8;
        static final int SNAPSHOT_MESSAGE_LENGTH = SNAPSHOT_PRICE_OFFSET + 8;

        private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
        private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();
        private final Auction auction = new Auction();
        private Cluster cluster;
        private IdleStrategy idleStrategy;

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                loadSnapshot(cluster, snapshotImage);
            }
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
            final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
            final long price = buffer.getLong(offset + PRICE_OFFSET);

            final boolean bidSucceeded = auction.attemptBid(price, customerId);

            if (null != session)
            {
                egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
                egressMessageBuffer.putLong(CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId());
                egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice());
                egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, bidSucceeded ? (byte)1 : (byte)0);

                idleStrategy.reset();
                while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0)
                {
                    idleStrategy.idle();
                }
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            snapshotBuffer.putLong(SNAPSHOT_CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId());
            snapshotBuffer.putLong(SNAPSHOT_PRICE_OFFSET, auction.getBestPrice());

            idleStrategy.reset();
            while (snapshotPublication.offer(snapshotBuffer, 0, SNAPSHOT_MESSAGE_LENGTH) < 0)
            {
                idleStrategy.idle();
            }
        }

        private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
        {
            final MutableBoolean isAllDataLoaded = new MutableBoolean(false);
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                final long customerId = buffer.getLong(offset + SNAPSHOT_CUSTOMER_ID_OFFSET);
                final long price = buffer.getLong(offset + SNAPSHOT_PRICE_OFFSET);

                auction.loadInitialState(price, customerId);

                isAllDataLoaded.set(true);
            };

            while (!snapshotImage.isEndOfStream())
            {
                final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 1);

                if (isAllDataLoaded.value)
                {
                    break;
                }

                idleStrategy.idle(fragmentsPolled);
            }
        }

        public void onRoleChange(final Cluster.Role newRole)
        {
        }

        public void onTerminate(final Cluster cluster)
        {
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
        }

        public void onSessionClose(
            final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
        }

        static class Auction
        {
            private long bestPrice = 0;
            private long currentWinningCustomerId = -1;

            void loadInitialState(final long price, final long customerId)
            {
                bestPrice = price;
                currentWinningCustomerId = customerId;
            }

            boolean attemptBid(final long price, final long customerId)
            {
                if (price <= bestPrice)
                {
                    return false;
                }

                bestPrice = price;
                currentWinningCustomerId = customerId;

                return true;
            }

            long getBestPrice()
            {
                return bestPrice;
            }

            long getCurrentWinningCustomerId()
            {
                return currentWinningCustomerId;
            }
        }
    }
}
