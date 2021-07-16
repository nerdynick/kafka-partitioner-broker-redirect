package org.apache.kafka.client.producer.oss;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.oos.OutOfSyncRedirectPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

public class TestOutOfSyncRedirectPartitioner {
    private static final byte[] testBytes = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vivamus cursus turpis eget ultrices feugiat. Aenean vestibulum tellus id tristique varius. Morbi congue dui risus, sed facilisis lectus semper a. Integer pellentesque ac risus nec tristique. Morbi augue nibh, imperdiet non rutrum id, laoreet vitae nisi. Nullam ipsum lorem, interdum quis purus non, tempor molestie dolor. Curabitur pharetra magna molestie nisl tempor, sed sagittis libero hendrerit. Vivamus imperdiet dapibus lacus at congue. Nam at enim molestie, convallis nibh sit amet, suscipit est. Nulla nibh augue, iaculis a mauris sit amet, posuere dictum velit. Donec consequat sem pharetra massa viverra interdum. Pellentesque vitae augue ullamcorper, vulputate tellus ac, pretium augue. Quisque et risus nec nunc tristique convallis sit amet eu mi. Nam ante nisl, fringilla vitae tincidunt vel, iaculis et ipsum. Sed nisl velit, scelerisque ut neque vel, commodo tempor libero. Quisque arcu ante, venenatis at urna vel, iaculis rutrum nunc. Vestibulum euismod a felis nec pretium. Praesent mollis euismod sem, sed efficitur turpis congue sit amet. Proin tincidunt pretium massa, in placerat justo lobortis eget. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Nunc consectetur dolor felis, ut pellentesque sapien dictum sit amet. Donec sodales augue in interdum pulvinar. Donec euismod ornare est, quis malesuada nunc. Donec in dapibus magna. Aenean malesuada lacus eros, eu blandit sapien bibendum quis. Sed iaculis, ex eget elementum dictum, urna massa vehicula sapien, quis imperdiet magna ligula nec neque. Praesent tempus, justo vehicula ullamcorper euismod, velit nulla interdum tortor, id sagittis ex purus at nisl. Nunc ipsum tellus, volutpat ut finibus in, ullamcorper sit amet orci. Pellentesque rhoncus sapien a volutpat rhoncus. Fusce eget posuere arcu. Praesent sagittis dui vitae tellus pulvinar molestie. Cras porttitor eros in sapien convallis, nec elementum odio dignissim. Duis tempor eget tellus id facilisis. Aenean posuere eu nibh in lacinia. Aliquam ullamcorper libero a est placerat, et iaculis augue cursus. Quisque sagittis tincidunt libero vel commodo. Cras porttitor eu risus vitae pharetra. Nunc condimentum a nunc aliquet accumsan. Nullam quis fringilla neque. Sed volutpat, risus ac vulputate feugiat, odio leo porta sem, consectetur sollicitudin tellus eros nec dui.Quisque nec molestie ipsum, ut sodales enim. Donec consectetur et lectus in ultricies. Nunc lorem tortor, sollicitudin a enim sed, iaculis auctor metus. Nullam non tincidunt lorem. Fusce rhoncus ligula quam, non posuere massa congue ut. Fusce at enim efficitur, bibendum leo vitae, semper risus. Mauris mollis nisi purus, ut fringilla augue pellentesque auctor. Nulla pulvinar ullamcorper mi quis finibus. Aliquam erat volutpat.".getBytes();
    public static final Cluster GoodCluster;
    public static final Cluster BadCluster;

    static {
        final Node[] nodes = new Node[]{
            new Node(1, "localhost", 9091),
            new Node(2, "localhost", 9092),
            new Node(3, "localhost", 9093)
        };

        final List<PartitionInfo> goodPartitions = Arrays.asList(
            new PartitionInfo("test", 0, nodes[0], nodes, nodes),
            new PartitionInfo("test", 1, nodes[1], nodes, nodes),
            new PartitionInfo("test", 2, nodes[2], nodes, nodes)
        );
        final List<PartitionInfo> badPartitions = Arrays.asList(
            new PartitionInfo("test", 0, nodes[0], nodes, nodes),
            new PartitionInfo("test", 2, nodes[1], nodes, new Node[]{nodes[1], nodes[2]}),
            new PartitionInfo("test", 3, nodes[2], nodes, new Node[]{nodes[1], nodes[2]})
        );

        GoodCluster = new Cluster("abc", Arrays.asList(nodes), goodPartitions, Collections.emptySet(), Collections.emptySet());
        BadCluster = new Cluster("abc", Arrays.asList(nodes), badPartitions, Collections.emptySet(), Collections.emptySet());
    }

    @Test
    public void testGoodTopic() throws InterruptedException, ExecutionException{
        Producer<byte[], byte[]> producer = new MockProducer<>(GoodCluster, true, new OutOfSyncRedirectPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());

        int count = 0;
        while(count < 10000){
            count++;
            Future<RecordMetadata> f = producer.send(new ProducerRecord<byte[],byte[]>("test", null, testBytes));
            RecordMetadata m = f.get();

            assertNotNull(m.partition());
            assertTrue(m.partition() >= 0 && m.partition() <=3);
        }

        producer.close();
    }

    @Test
    public void testBadTopic() throws InterruptedException, ExecutionException{
        Producer<byte[], byte[]> producer = new MockProducer<>(BadCluster, true, new OutOfSyncRedirectPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());

        int count = 0;
        while(count < 10000){
            count++;
            Future<RecordMetadata> f = producer.send(new ProducerRecord<byte[],byte[]>("test", null, testBytes));
            RecordMetadata m = f.get();

            assertNotNull(m.partition());
            assertTrue(m.partition() >= 1 && m.partition() <=3, "Partition was an invalid one");
        }

        producer.close();
    }

    @Test
    public void testMultiThread() throws InterruptedException, ExecutionException{
        final Producer<byte[], byte[]> producer = new MockProducer<>(BadCluster, true, new OutOfSyncRedirectPartitioner(), new ByteArraySerializer(), new ByteArraySerializer());
        final CountDownLatch countDownLatch = new CountDownLatch(10);

        for(int threads = 0; threads < 10; threads++){
            new Thread(new Runnable(){
                @Override
                public void run() {
                    for(int records = 0; records < 100; records++){
                        Future<RecordMetadata> f = producer.send(new ProducerRecord<byte[],byte[]>("test", null, testBytes));
                        RecordMetadata m;
                        try {
                            m = f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }

                        assertNotNull(m.partition());
                        assertTrue(m.partition() >= 1 && m.partition() <=3, "Partition was an invalid one");
                    }
                    countDownLatch.countDown();
                }
            }).start();
        }

        
        countDownLatch.await();
        producer.close();
    }
}
