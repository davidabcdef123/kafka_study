package com;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by sc on 2018/12/23.
 */
public class KafkaConsumerDemo extends Thread {

    private final KafkaConsumer kafkaConsumer;

    public KafkaConsumerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.80.135:9092,192.168.80.136:9092,192.168.80.137:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaProducerDemo");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<Integer, String> consumerRecor = kafkaConsumer.poll(1000);
            for (ConsumerRecord record : consumerRecor) {
                System.out.println("message receive:"+record.value());
                kafkaConsumer.commitAsync();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaConsumerDemo(KafkaProducerDemo.TEST_TOPIC).start();

    }
}
