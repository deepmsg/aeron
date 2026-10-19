/*
 * A reference container the fixture can point at any Java business.
 *
 * The LC-4a scenarios have two arms that are not ours: the "all Java"
 * self-check (the same scenario with the reference's own container in place of
 * ours) and the reference's container as the other half of a comparison. Both
 * need the same thing — `ClusteredServiceContainer` with the business named on
 * the command line — which is what this probe is. It is the generic form of
 * `ClusterAuctionServiceProbe`, which hard-codes one business.
 *
 * The class is loaded the way `aeron.cluster.service.class.name` would load it
 * (`ClusteredServiceContainer.java:673-687`): a public class with a public
 * no-argument constructor. Loading it here rather than passing the property
 * keeps the class on this probe's own class path, which is the jar plus the
 * compiled probes directory.
 *
 * Usage: <aeron dir> <cluster dir> <cluster id> <service id> <class name>
 *        <isRespondingService> <control channel> [cluster mark dir]
 *
 * Compiled by the test:
 *     javac -proc:none -cp <aeron-all.jar> -d <classes> ClusterServiceContainerProbe.java
 */

import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import java.lang.reflect.Constructor;

public class ClusterServiceContainerProbe
{
    public static void main(final String[] args) throws Exception
    {
        final String aeronDir = args[0];
        final String clusterDir = args[1];
        final int clusterId = Integer.parseInt(args[2]);
        final int serviceId = Integer.parseInt(args[3]);
        final String className = args[4];
        final boolean isRespondingService = Boolean.parseBoolean(args[5]);
        final String controlChannel = args[6];

        final Constructor<?> constructor = Class.forName(className).getConstructor();
        final ClusteredService service = (ClusteredService)constructor.newInstance();

        final ClusteredServiceContainer.Context context = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDirectoryName(clusterDir)
            .clusterId(clusterId)
            .serviceId(serviceId)
            .controlChannel(controlChannel)
            .clusteredService(service)
            .isRespondingService(isRespondingService);

        final ClusteredServiceContainer container = ClusteredServiceContainer.launch(context);
        Runtime.getRuntime().addShutdownHook(new Thread(container::close));
        System.out.println("STARTED " + serviceId + " " + className);

        Thread.currentThread().join();
    }
}
