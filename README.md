# kafka-partitioner-broker-redirect

A collection of ReRouting/Redirecting Kafka Partitioner Implementations.

# Implementations

## OutOfSyncRedirectPartitioner

Implementation of Partitioner that uses OutOfSyncRedirectStickyPartitionCache to redirect records away from Brokers that have out of sync replicas for the Topic that is being published too.

The idea is to give the slow broker the ability to catch up on replication and Publish requests by redirect away new records from being pushed to it. 
Until the replicas are considered In Sync.

### Example

```java
final Map<String, Object> configs = new HashMap<>();
configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "MY-BOOTSRAP");

//OPTIONAL: We reduce the Metadata Max Age in order to reduce the interval at which we detect the Out-of-Sync Replicas.
//          The default is 5mins. 
//IMPORTANT: This will have a performance cost. 
//           As this results in more network/request calls being made to the brokers to get updated metadata more frequently.
configs.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, Duration.ofSeconds(60).toMillis());

//Set the configured Partitioner to this implementation
configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OutOfSyncRedirectPartitioner.class);

final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configs, new ByteArraySerializer(), new ByteArraySerializer());
```
## OutOfSyncProxyRedirectPartitioner

Implementation of Partitioner that uses OutOfSyncRedirectStickyPartitionCache to redirect records away from Brokers that have out of sync replicas for a Proxy Topic.
This is similar to `OutOfSyncRedirectPartitioner`, but instead of using the topic that is being written to. It uses a separate "proxy" topic to evaluate against. 

### Example

```java
final Map<String, Object> configs = new HashMap<>();
configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "MY-BOOTSRAP");

//OPTIONAL: We reduce the Metadata Max Age in order to reduce the interval at which we detect the Out-of-Sync Replicas.
//          The default is 5mins. 
//IMPORTANT: This will have a performance cost. 
//           As this results in more network/request calls being made to the brokers to get updated metadata more frequently.
configs.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, Duration.ofSeconds(60).toMillis());

//Set the configured Partitioner to this implementation
configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OutOfSyncProxyRedirectPartitioner.class);
configs.put(OutOfSyncProxyRedirectStickyPartitionCache.Config.PROXY_TOPIC_CONFIG, "MY_PROXY_TOPIC");

final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configs, new ByteArraySerializer(), new ByteArraySerializer());
```