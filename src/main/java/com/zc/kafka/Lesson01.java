package com.zc.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Title:  Lesson01
 * Description:
 *
 * @author ext.zhuochen
 * @data 2021-03-30 17:25
 **/
public class Lesson01 {

    /**
     *  创建topic
     * ./kafka-topics.sh --zookeeper 47.94.84.253:2181/kafka-0 --create --topic zc-items --partitions 1 --replication-factor 1
     */

    @Test
    public void producer() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        // properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"iZ2ze0m18xa3fxl5t2qq8rZ:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"iZ2ze0m18xa3fxl5t2qq8rZ:9092");
        // kafka 持久化数据的MQ 数据-> byte[],不会对数据进行干预双方要约定编解码
        // kafka是一个app，使用零拷贝，sendfile 系统调用实现是快速数据消费。
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);

        // 现在的 producer就是一个提供者，面向的其实是broker，虽然再使用的时候我们期望把数据打入topic
        /*
        zc-items
        1 partition
        三种商品，每种商品有线性的3个ID
        相同的商品最好去到一个分区里
         */
        String topic = "zc-items";
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,"ITEM" + j, "val" + i);
                Future<RecordMetadata> send = stringStringKafkaProducer.send(record);
                RecordMetadata recordMetadata = send.get();
                int partition = recordMetadata.partition();
                long offset = recordMetadata.offset();
                System.out.println("key:"+record.key()+ " val:"+record.value()+" partition:"+partition+" offset"+offset);

            }
        }


    }


    @Test
    public void consumer(){
        /**
         * ./kafka-consumer-groups.sh --bootstrap-server iZ2ze0m18xa3fxl5t2qq8rZ:9092 --describe --group zc
         */

        // 基础配置
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"iZ2ze0m18xa3fxl5t2qq8rZ:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费的细节
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"zc-1");
        // KAFKA IS MQ STRAGE
        /**
         * What to do when there is no initial offset in Kafka or
         * if the current offset does not exist any more on the server (e.g. because that data has been deleted):
         * <ul>
         *     <li>earliest: automatically reset the offset to the earliest offset<li>
         *     <li>latest:automatically reset the offset to the latest offset</li>
         *     <li>none:throw exception to the consumer if no previous offset is found for the consumer's group</li>
         *     <li>anything else:throw exception to the consumer.</li>
         * </ul>
         */
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 一个运行的consumer，那么自己会维护自己消费进度
        // 一旦你自动提交，但是是异步的
        // 1.还没到时间，挂了，没提交，重起一个consumer，参考offset的时候会重复消费
        // 2.一个批次的数据还没写数据库成功，但是这个批次的offset被异步提交，挂了，重起一个consumer，参考offset的时候会丢失消费

        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true"); // 自动提交是异步提交。丢数据&& 重复数据
        // p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,) // 提交间隔5秒
        // p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,""); //  POLL拉取数据，弹性，按需，拉取多少。

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        String topic = "zc-items";
        // kafka 的consumer会动态负载均衡。
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsRevoked---");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next().partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("---onPartitionsAssigned---");
                Iterator<TopicPartition> iterator = partitions.iterator();
                while (iterator.hasNext()){
                    System.out.println(iterator.next().partition());
                }
            }
        });


        while (true){
            /**
             * 常识：如果多线程处理多分区
             * 每poll一次，用一个语义，一个job启动
             * 一次job用多线程并行处理分区
             * 且，job应该被控制是串行。
             * 以上的知识点，其实如果你学过大数据，
             */


            // 微批的感觉
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(0)); // 0 ~ n
            if(!consumerRecords.isEmpty()){



                Set<TopicPartition> partitions = consumerRecords.partitions();// 每次poll的时候是取多个分区的数据。
                /**
                 * 如果手动提交offset
                 * 1. 按消息进度同步提交
                 * 2. 按分区力度同步提交
                 * 3. 按当前poll的批次同步提交
                 *
                 * 思考：如果再多个线程下
                 * 1.以上1，3的方式不用多线程
                 * 2. 以上2 的方式最容易想到多线程方式处理，有没有问题？
                 *
                 */

                // 每个分区内的数据是有序的。
                for (TopicPartition topicPartition:partitions) {
                    List<ConsumerRecord<String, String>> recordsOne = consumerRecords.records(topicPartition);
                    // 在一个维批里，按分区回去poll回来的数据
                    // 线性按分区处理，还可以并行按分区处理用多线程的方式
                    Iterator<ConsumerRecord<String, String>> iterator = recordsOne.iterator();
                    while (iterator.hasNext()){
                        ConsumerRecord<String, String> record = iterator.next();
                        int partition = record.partition();
                        long offset = record.offset();
                        String key = record.key();
                        String value = record.value();
                        System.out.println("key:"+record.key()+" val:"+record.value()+" partition"+partition+" offset"+offset);

                        // TopicPartition topicPartition1 = new TopicPartition(topic, partition);
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap = new HashMap<>();
                        topicPartitionOffsetAndMetadataHashMap.put(topicPartition,offsetAndMetadata);
                        /**
                         * onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception)
                         */
                        consumer.commitAsync(topicPartitionOffsetAndMetadataHashMap, (offsets, exception) -> {
                        }); // 这个是最安全的，每条记录级的更新，第一点
                        // 单线程，多线程，都可以。
                    }
                    long offset = recordsOne.get(recordsOne.size() - 1).offset();
                    OffsetAndMetadata om = new OffsetAndMetadata(offset);
                    HashMap<TopicPartition, OffsetAndMetadata> tm = new HashMap<>();
                    tm.put(topicPartition,om);

                    consumer.commitAsync(tm, (offsets, exception) -> {}); // 这个是第二种分区粒度提交offset
                    /**
                     * 因为你都分区了
                     * 拿到分区的数据集
                     * 期望的是先对数据整体加工
                     * 小问题会出现？你怎么知道最后一条小的offset！！！！
                     * 感觉一定要有，kafka很傻，你拿走了多少，我不关心，你告诉我你正确的最后一个小的offset。
                     */

                }
                consumer.commitAsync(); // 这个就是按poll的批次提交offset，第三点
                // 以下代码的优化很重要
                System.out.println("----------------------"+consumerRecords.count()+"----------------------------------");
               /* Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
                while (iterator.hasNext()){
                    // 因为一个consumer可以消费多个分区，但是一个分区只能给一个组里的一个cinsumer消费
                    ConsumerRecord<String, String> record = iterator.next();
                    int partition = record.partition();
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();

                    System.out.println("key:"+record.key()+" val:"+record.value()+" partition"+partition+" offset"+offset);
                }*/
            }

        }



    }


}
