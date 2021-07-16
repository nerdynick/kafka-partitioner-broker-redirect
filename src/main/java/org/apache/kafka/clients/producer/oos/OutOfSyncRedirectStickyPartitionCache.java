package org.apache.kafka.clients.producer.oos;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modified version of {@link org.apache.kafka.clients.producer.internals.StickyPartitionCache} that reduces what is considered an Available partition.
 * It does this by removing any partitions whos leader might also have replica for the same topic that are not in sync with the leader of that partition.
 * 
 * @see org.apache.kafka.clients.producer.internals.StickyPartitionCache
 */
public class OutOfSyncRedirectStickyPartitionCache extends AbstractOutOfSyncRedirectPartitionCache {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOutOfSyncRedirectPartitionCache.class);

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Using Normal OutOfSync Partitioning");
    }

    @Override
    List<PartitionInfo> computeAvailablePartitions(String topic, Cluster cluster) {
        LOG.debug("Computing Available Partitions");
        return this.computeAvailablePartitions(topic, cluster, this.findOutOfSyncNodes(topic, cluster));
    }
}
