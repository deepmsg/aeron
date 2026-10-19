/*
 * The client half of P3-2b's snapshot card: the reference's own `AeronCluster`
 * sends the tutorial's bids, reads what the auction answered, and — when asked
 * — turns the consensus module's SNAPSHOT toggle.
 *
 * The business it talks to is `BasicAuctionClusteredService`, on the Java side
 * of the twin comparison in `ClusterAuctionServiceProbe`, and the Rust side in
 * `deepmsg_cluster_service::services::BasicAuction`. The record layouts are the
 * reference's:
 *
 *     bid  (24 B): correlationId i64 @0, customerId i64 @8, price i64 @16
 *     egress (25 B): correlationId @0, winningCustomerId @8, bestPrice @16,
 *                    bidSucceeded u8 @24
 *
 * It prints one reading and exits 0 whatever the count turned out to be — the
 * test is what decides whether the reading is a pass:
 *
 *     RECEIVED <n>
 *     LAST <winningCustomerId> <bestPrice> <bidSucceeded>
 *
 * and, when the snapshot argument is true:
 *
 *     SNAPSHOT_REQUESTED
 *
 * `OFFER_FAILED <code>` or `NO_TOGGLE` is printed instead when the cluster
 * never accepted a message or the control toggle is missing.
 *
 * Usage: <aeron dir> <cluster id> <bid count> <customer id> <first price>
 *        [snapshot] [hold millis]
 *
 * `hold millis` keeps the client (and its session) open after the reading is
 * printed, which is how a test makes the snapshot's session table non-empty.
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterAuctionBidClientProbe.java
 */

import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

public class ClusterAuctionBidClientProbe
{
    private static final int INGRESS_STREAM_ID = 101;
    private static final int EGRESS_STREAM_ID = 102;
    private static final int BID_LENGTH = 24;
    private static final int EGRESS_LENGTH = 25;
    private static final int WINNING_CUSTOMER_ID_OFFSET = 8;
    private static final int BEST_PRICE_OFFSET = 16;
    private static final int BID_SUCCEEDED_OFFSET = 24;
    private static final long TIMEOUT_SECONDS = 30;

    public static void main(final String[] args)
    {
        final String aeronDir = args[0];
        final int clusterId = Integer.parseInt(args[1]);
        final int bids = Integer.parseInt(args[2]);
        final long customerId = Long.parseLong(args[3]);
        final long firstPrice = Long.parseLong(args[4]);
        final boolean snapshot = args.length > 5 && Boolean.parseBoolean(args[5]);
        final long holdMillis = args.length > 6 ? Long.parseLong(args[6]) : 0;

        final long[] winningCustomerId = new long[] { -2 };
        final long[] bestPrice = new long[] { -2 };
        final boolean[] succeeded = new boolean[] { false };
        final int[] received = new int[] { 0 };

        try (AeronCluster cluster = AeronCluster.connect(
            new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .ingressChannel("aeron:ipc")
                .ingressStreamId(INGRESS_STREAM_ID)
                .egressChannel("aeron:ipc")
                .egressStreamId(EGRESS_STREAM_ID)
                .egressListener((sessionId, timestamp, buffer, offset, length, header) ->
                {
                    if (length >= EGRESS_LENGTH)
                    {
                        winningCustomerId[0] =
                            buffer.getLong(offset + WINNING_CUSTOMER_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
                        bestPrice[0] =
                            buffer.getLong(offset + BEST_PRICE_OFFSET, ByteOrder.LITTLE_ENDIAN);
                        succeeded[0] = 0 != buffer.getByte(offset + BID_SUCCEEDED_OFFSET);
                    }
                    received[0]++;
                })))
        {
            final UnsafeBuffer bid = new UnsafeBuffer(new byte[BID_LENGTH]);
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);

            for (int i = 0; i < bids; i++)
            {
                bid.putLong(0, i, ByteOrder.LITTLE_ENDIAN);
                bid.putLong(8, customerId, ByteOrder.LITTLE_ENDIAN);
                bid.putLong(16, firstPrice + i, ByteOrder.LITTLE_ENDIAN);

                long result;
                while ((result = cluster.offer(bid, 0, bid.capacity())) < 0)
                {
                    if (System.nanoTime() > deadline)
                    {
                        System.out.println("OFFER_FAILED " + result);
                        return;
                    }
                    cluster.pollEgress();
                    Thread.yield();
                }

                while (received[0] <= i && System.nanoTime() < deadline)
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }

            System.out.println("RECEIVED " + received[0]);
            System.out.println("LAST " + winningCustomerId[0] + " " + bestPrice[0] + " " + succeeded[0]);

            if (snapshot)
            {
                final CountersReader counters =
                    ClusterControl.mapCounters(new File(aeronDir, "cnc.dat"));
                final AtomicCounter controlToggle =
                    ClusterControl.findControlToggle(counters, clusterId);
                if (null == controlToggle)
                {
                    System.out.println("NO_TOGGLE");
                    return;
                }

                if (!ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle))
                {
                    System.out.println("TOGGLE_REFUSED");
                    return;
                }
                System.out.println("SNAPSHOT_REQUESTED");

                // Stay connected for a moment: the snapshot is taken on the
                // module's next turns, and the session should be open when it is.
                final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                while (System.nanoTime() < until)
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }

            if (holdMillis > 0)
            {
                final long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(holdMillis);
                while (System.nanoTime() < until)
                {
                    cluster.pollEgress();
                    Thread.yield();
                }
            }
        }
    }
}
