package org.apache.kafka.clients.producer.oos;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link org.apache.kafka.clients.producer.Partitioner} that uses {@link org.apache.kafka.clients.producer.oos.OutOfSyncRedirectStickyPartitionCache}
 * to redirect records away from Brokers that have out of sync replicas for the Topic that is being published too.
 * 
 * The idea is to give the slow broker the ability to catch up on replication and Publish requests
 * by redirect away new records from being pushed to it. Until the replicas are considered In Sync.
 * 
 * @see org.apache.kafka.clients.producer.Partitioner
 * @see org.apache.kafka.clients.producer.oos.OutOfSyncRedirectStickyPartitionCache
 */
public class OutOfSyncRedirectPartitioner implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(OutOfSyncRedirectPartitioner.class);

    final OutOfSyncRedirectStickyPartitionCache uStickyPartitionCache = new OutOfSyncRedirectStickyPartitionCache();

    @Override
    public void configure(Map<String, ?> configs) {
        Properties p = new Properties();
        p.putAll(configs);

        ProducerConfig c = new ProducerConfig(p);

        Integer deliveryTimeout = c.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        Long metadatTTL = c.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG);

        LOG.info("MetadataTTL=`{}` DeliveryTimeout=`{}`", metadatTTL, deliveryTimeout);
        if (metadatTTL >= deliveryTimeout){
            LOG.warn("Metadata TTL is Greater >= Delivery Timeout. The can result in increased delayed response to ISR issues and more TimedOut records to handle.");
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return uStickyPartitionCache.partition(topic, cluster);
    }

    @Override
    public void close() {}

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        uStickyPartitionCache.nextPartition(topic, cluster, prevPartition);
    }
}
