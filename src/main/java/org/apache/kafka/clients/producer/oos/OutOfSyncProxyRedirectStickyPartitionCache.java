package org.apache.kafka.clients.producer.oos;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modified version of {@link org.apache.kafka.clients.producer.internals.StickyPartitionCache} that reduces what is considered an Available partition.
 * It does this by removing any partitions whos leader might also have replica for a proxy topic that are not in sync with the leader of that partition.
 * 
 * @see org.apache.kafka.clients.producer.internals.StickyPartitionCache
 */
public class OutOfSyncProxyRedirectStickyPartitionCache extends AbstractOutOfSyncRedirectPartitionCache {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOutOfSyncRedirectPartitionCache.class);

    private String proxyTopic;

    @Override
    public void configure(Map<String, ?> configs) {
        LOG.info("Using Proxied OutOfSync Partitioning");

        Config cnf = new Config(configs, true);
        proxyTopic = cnf.getProxyTopic();
    }

    @Override
    List<PartitionInfo> computeAvailablePartitions(String topic, Cluster cluster) {
        LOG.debug("Computing Available Partitions");
        return this.computeAvailablePartitions(topic, cluster, this.findOutOfSyncNodes(proxyTopic, cluster));
    }


    public static class Config extends AbstractConfig {
        public static final String PROXY_TOPIC_CONFIG = "partitioner.proxy.topic";

        final static ConfigDef CONFIG;
        static {
            CONFIG = new ConfigDef(ProducerConfig.configDef())
                .define(PROXY_TOPIC_CONFIG, Type.STRING, Importance.HIGH, "Kafka Topic to use as a Proxy topic to detect invalid Nodes to exclude from Publishing to");
        }

        public Config(Map<?, ?> props, boolean doLog) {
            super(CONFIG, props, doLog);
        }

        public String getProxyTopic(){
            return this.getString(PROXY_TOPIC_CONFIG);
        }
        
    }
}
