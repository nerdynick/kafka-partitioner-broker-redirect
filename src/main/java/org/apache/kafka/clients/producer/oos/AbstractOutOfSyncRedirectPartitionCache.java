package org.apache.kafka.clients.producer.oos;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutOfSyncRedirectPartitionCache implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOutOfSyncRedirectPartitionCache.class);

    private final ConcurrentMap<String, Integer> indexCache = new ConcurrentHashMap<>();

    /**
     * Either computes a new partition or returns the current partition to be publishing too.
     * 
     * @param topic Topic to Partition against
     * @param cluster Cluster Metadata state
     * @return New or Cached partition ID
     */
    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        if (part == null) {
            part = nextPartition(topic, cluster, -1);
        }
        LOG.debug("Partition is `{}`", part);
        return part;
    }

    /**
     * Moves the index to a new computed one. 
     * Not ment to be called directly when look to get a partition to send a record to.
     * 
     * @param topic Topic to Partition against
     * @param cluster Cluster Metadata state
     * @param prevPartition Last partition to be written to or -1
     * @return New partition ID
     */
    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        LOG.debug("Computing new Partition");
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that 
        // triggered the new batch matches the sticky partition that needs to be changed.
        if (oldPart == null || oldPart == prevPartition) {
            List<PartitionInfo> availablePartitions = this.computeAvailablePartitions(topic, cluster);
            if (availablePartitions.size() < 1) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition();
            } else {
                while (newPart == null || newPart.equals(oldPart)) {
                    Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }

    protected Set<Node> findOutOfSyncNodes(String topic, Cluster cluster){
        final List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        final Set<Node> nodesOutOfSync = new HashSet<>();

        //Find node that have out of sync partitions
        for(PartitionInfo i: availablePartitions){
            final Node[] isr = i.inSyncReplicas();
            for(Node n: i.replicas()){
                boolean found = false;
                for(Node iNode: isr){
                    if (iNode.equals(n)){
                        found = true;
                        break;
                    }
                }
                if (!found){
                    LOG.info("Found Broker Node with out of sync replica: N={} P={}", n, i.partition());
                    nodesOutOfSync.add(n);
                }
            }
        }

        return nodesOutOfSync;
    }

    /**
     * Evaulates the given state of the world and finds what partitions are available base on weather they are Offline or also contain OutOfSync replicas.
     * OutOfSync Replicas are based on evaluating all leaders and replicas looking for a leader of 1 partition that has replicas for other partitions. 
     * If any of those other replicas are not InSync. 
     * Then that leader is consisered to not be available and there for all partitions it's a leader for to not be available. 
     * 
     * @param topic Topic to Partition against
     * @param cluster Cluster Metadata state
     * @return New partition ID
     */
    protected List<PartitionInfo> computeAvailablePartitions(final String topic, final Cluster cluster, final Set<Node> nodesOutOfSync){
        LOG.debug("Computing Available Partitions based on InSyncReplicas");
        final List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
        final List<PartitionInfo> inSyncAvailablePartitions = new LinkedList<>();

        //Find available partitions based on our leaders being in sync with all it's replicas
        for(PartitionInfo i: availablePartitions){
            if(!nodesOutOfSync.contains(i.leader())){
                LOG.info("Parition is Valid: P=`{}` Leader=`{}`", i.partition(), i.leader());
                inSyncAvailablePartitions.add(i);
            } else {
                LOG.warn("Parition is InValid, Node has out of sync replica(s): P={} N={}", i.partition(), i.leader());
            }
        }

        return inSyncAvailablePartitions;
    }

    abstract List<PartitionInfo> computeAvailablePartitions(String topic, Cluster cluster);
}
