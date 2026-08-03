package com.coreplantform.daemonproceed.facdeImpl.utils.kafka.implement;

import com.coreplantform.daemonproceed.facdeImpl.utils.kafka.interfaces.CommunicationConsumer;
import com.coreplantform.daemonproceed.facdeImpl.utils.kafka.interfaces.model.TimeStampMessage;
import com.coreplantform.daemonproceed.facdeImpl.utils.kafka.serialization.NoDeserialization;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class KafkaConsumerImpl implements CommunicationConsumer {
    private KafkaConsumer<String, Byte[]> kafkaConsumer;
    private long pollTimeout;

    @Override
    public void initial(Properties properties, long pollTimeout) {
        this.pollTimeout = pollTimeout;
        configureConsumer(properties);
    }

    @Override
    public void initial(String configPath, long pollTimeout) {
        this.pollTimeout = pollTimeout;
        try (FileInputStream inputStream = new FileInputStream(configPath)) {
            Map<String, Object> rootConfig = new Yaml().load(inputStream);
            Map<String, Object> kafkaConfig = castMap(rootConfig.get("kafka"));
            Map<String, Object> consumerConfig = castMap(kafkaConfig.get("consumer"));
            Properties properties = new Properties();
            properties.setProperty(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    String.valueOf(consumerConfig.get("bootstrap-servers"))
            );
            properties.setProperty(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    String.valueOf(consumerConfig.get("key-deserializer"))
            );
            properties.setProperty(
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    String.valueOf(consumerConfig.get("auto-offset-reset"))
            );
            properties.setProperty(
                    ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                    String.valueOf(consumerConfig.get("max-poll-records"))
            );
            properties.setProperty(
                    ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                    String.valueOf(consumerConfig.get("max-poll-interval-ms"))
            );
            properties.setProperty(
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                    String.valueOf(consumerConfig.get("session-timeout-ms"))
            );
            properties.setProperty(
                    ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                    String.valueOf(consumerConfig.get("heartbeat-interval-ms"))
            );
            configureConsumer(properties);
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    private void configureConsumer(Properties properties) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NoDeserialization.class.getName());
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        setDefault(properties, ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
        setDefault(properties, ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        setDefault(properties, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    private static void setDefault(Properties properties, String key, String value) {
        if (!properties.containsKey(key)) {
            properties.setProperty(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        return (Map<String, Object>) value;
    }

    @Override
    public void subscribe(String topicName) {
        kafkaConsumer.subscribe(Collections.singleton(topicName));
    }

    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }

    @Override
    public Byte[] receive() {
        ConsumerRecords<String, Byte[]> records = kafkaConsumer.poll(Duration.ofMillis(pollTimeout));
        if (records.isEmpty()) {
            return new Byte[0];
        }
        ConsumerRecord<String, Byte[]> record = records.iterator().next();
        return record.value();
    }

    @Override
    public TimeStampMessage timeStampReceive() {
        ConsumerRecords<String, Byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(1));
        if (records.isEmpty()) {
            return new TimeStampMessage();
        }
        ConsumerRecord<String, Byte[]> record = records.iterator().next();
        TimeStampMessage message = new TimeStampMessage();
        message.setTimeStamp(record.timestamp());
        message.setMessageValue(record.value());
        return message;
    }

    @Override
    public void clear() {
        close();
        kafkaConsumer = null;
        pollTimeout = 0;
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
