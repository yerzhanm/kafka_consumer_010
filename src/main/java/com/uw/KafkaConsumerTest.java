package com.uw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaConsumerTest {

    final String brokers="localhost:9092";
    final String zookeepers="localhost:2181";
    final String topic= "testTopic";
    final String consumerGroup="group";

    public static void main(String args[]){
        KafkaConsumerTest t = new KafkaConsumerTest();
        List<String> resultList = t.getLatest(100);
        System.out.println("size : "+resultList.size());
        for(String s : resultList){
            System.out.println(s);
        }

    }

    public List<String> getLatest(int limit){
        if(limit<=0) return new ArrayList<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", consumerGroup);
        props.put("zookeeper.connect", zookeepers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer consumer = new KafkaConsumer(props);

        List<String> result = new ArrayList<>(limit);
        List<Message> messages = new ArrayList<>();

        try {
            List<PartitionInfo> partInfoList = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>();
            for(PartitionInfo pi : partInfoList){
                TopicPartition tp = new TopicPartition(topic, pi.partition());
                partitions.add(tp);
            }
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);

            for(TopicPartition tp :partitions){
                long offset = (consumer.position(tp) < limit) ? consumer.position(tp) : limit;
                consumer.seek(tp, consumer.position(tp)-offset);
            }

            ConsumerRecords<String, String> records = consumer.poll(60000);
            for (ConsumerRecord<String, String> record : records) {
                messages.add(new Message(record.value(), record.timestamp()));
            }

        } finally {
            consumer.close();
        }
        Collections.sort(messages);

        int startIndex = (messages.size()-limit) <= 0 ? 0 : messages.size()-limit;

        for(int i = startIndex; i<messages.size(); i++){
            result.add(messages.get(i).value);
        }

        return result;
    }

    class Message implements Comparable<Message>{
        long timestamp;
        String value;

        public Message(String value, long timestamp){
            this.value = value;
            this.timestamp = timestamp;
        }

        public int compareTo(Message o) {
            return Long.compare(this.timestamp, o.timestamp);
        }
    }
}
