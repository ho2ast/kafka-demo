package com.ho2ast.kafkademo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("bootstrap.servers", "ho2ast:9092");
        config.put("session.timeout.ms", "10000");
        config.put("group.id", "test-topic");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");    // key deserializer
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // value deserializer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(Arrays.asList("test-topic"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500L));
            for (ConsumerRecord<String, String> record : records) {
                String input = record.topic();
                if ("test-topic".equals(input)) {
                    System.out.println("record = " + record);
//                    System.out.println("record.key()  = " + record.key());
//                    System.out.println("record.value() = " + record.value());
                } else {
                    throw new IllegalStateException("get message on topic" + record.topic());
                }
            }
        }
    }
}
