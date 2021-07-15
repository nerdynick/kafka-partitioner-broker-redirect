package org.apache.kafka.client.producer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.oos.OutOfSyncRedirectPartitioner;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class TestProducer {
    private final static AtomicBoolean shutdown = new AtomicBoolean(false);

    private static final byte[] testBytes = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus cursus turpis eget ultrices feugiat. Aenean vestibulum tellus id tristique varius. Morbi congue dui risus, sed facilisis lectus semper a. Integer pellentesque ac risus nec tristique. Morbi augue nibh, imperdiet non rutrum id, laoreet vitae nisi. Nullam ipsum lorem, interdum quis purus non, tempor molestie dolor. Curabitur pharetra magna molestie nisl tempor, sed sagittis libero hendrerit. Vivamus imperdiet dapibus lacus at congue. Nam at enim molestie, convallis nibh sit amet, suscipit est. Nulla nibh augue, iaculis a mauris sit amet, posuere dictum velit. Donec consequat sem pharetra massa viverra interdum. Pellentesque vitae augue ullamcorper, vulputate tellus ac, pretium augue. Quisque et risus nec nunc tristique convallis sit amet eu mi. Nam ante nisl, fringilla vitae tincidunt vel, iaculis et ipsum. Sed nisl velit, scelerisque ut neque vel, commodo tempor libero. Quisque arcu ante, venenatis at urna vel, iaculis rutrum nunc. Vestibulum euismod a felis nec pretium. Praesent mollis euismod sem, sed efficitur turpis congue sit amet. Proin tincidunt pretium massa, in placerat justo lobortis eget. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nunc consectetur dolor felis, ut pellentesque sapien dictum sit amet. Donec sodales augue in interdum pulvinar. Donec euismod ornare est, quis malesuada nunc. Donec in dapibus magna. Aenean malesuada lacus eros, eu blandit sapien bibendum quis. Sed iaculis, ex eget elementum dictum, urna massa vehicula sapien, quis imperdiet magna ligula nec neque. Praesent tempus, justo vehicula ullamcorper euismod, velit nulla interdum tortor, id sagittis ex purus at nisl. Nunc ipsum tellus, volutpat ut finibus in, ullamcorper sit amet orci. Pellentesque rhoncus sapien a volutpat rhoncus. Fusce eget posuere arcu. Praesent sagittis dui vitae tellus pulvinar molestie. Cras porttitor eros in sapien convallis, nec elementum odio dignissim. Duis tempor eget tellus id facilisis. Aenean posuere eu nibh in lacinia. Aliquam ullamcorper libero a est placerat, et iaculis augue cursus. Quisque sagittis tincidunt libero vel commodo. Cras porttitor eu risus vitae pharetra. Nunc condimentum a nunc aliquet accumsan. Nullam quis fringilla neque. Sed volutpat, risus ac vulputate feugiat, odio leo porta sem, consectetur sollicitudin tellus eros nec dui.Quisque nec molestie ipsum, ut sodales enim. Donec consectetur et lectus in ultricies. Nunc lorem tortor, sollicitudin a enim sed, iaculis auctor metus. Nullam non tincidunt lorem. Fusce rhoncus ligula quam, non posuere massa congue ut. Fusce at enim efficitur, bibendum leo vitae, semper risus. Mauris mollis nisi purus, ut fringilla augue pellentesque auctor. Nulla pulvinar ullamcorper mi quis finibus. Aliquam erat volutpat.".getBytes();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown.set(true);
            }
        });


        Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kfk01.nikoleta.aws.ps.confluent.io:9091,kfk02.nikoleta.aws.ps.confluent.io:9091,kfk03.nikoleta.aws.ps.confluent.io:9091");
        configs.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, Duration.ofSeconds(60).toMillis());
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1000);
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OutOfSyncRedirectPartitioner.class);
        
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configs, new ByteArraySerializer(), new ByteArraySerializer());

        while(!shutdown.get()){
            System.out.println("Getting Partition Info: test");

            List<PartitionInfo> info = producer.partitionsFor("test");

            System.out.println("Got Info");

            for(PartitionInfo i: info){
                // System.out.println(String.format("Partition=%s\n\tReplicas: %s\n\tISR: %s\n\tOfflineReplicas: %s)", 
                //     i.partition(), 
                //     Arrays.toString(i.replicas()), 
                //     Arrays.toString(i.inSyncReplicas()), 
                //     Arrays.toString(i.offlineReplicas())
                // ));

                System.out.println(String.format("Partition=`%s` ISR=`%s` Leader=`%s`", i.partition(), i.inSyncReplicas().length, i.leader()));
            }

            System.out.println("Producing Record");
            producer.send(new ProducerRecord<byte[],byte[]>("test", null, testBytes), new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null){
                        System.out.println(String.format("Exception: %s\tMessage: %s", exception.getClass(), exception.getMessage()));
                    }
                }
            });

            try {
                Thread.sleep(Duration.ofSeconds(10).toMillis());
            } catch (InterruptedException e) {
                shutdown.set(true);
                break;
            }
        }

        producer.close();
    }
}
