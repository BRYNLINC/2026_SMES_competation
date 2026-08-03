package com.coreplantform.daemonproceed;

import com.coreplantform.daemonproceed.config.KafkaProducerConfigProperties;
import java.io.File;
import java.util.UUID;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@SpringBootApplication
@EnableConfigurationProperties(KafkaProducerConfigProperties.class)
public class ProceedApplication {
    private static final int ZOOKEEPER_CONNECTION_TIMEOUT_MS = 30000;
    private static final int ZOOKEEPER_SESSION_TIMEOUT_MS = 60000;

    public static void main(String[] args) {
        File kafkaLogDirectory = createKafkaLogDirectory();
        EmbeddedKafkaBroker kafkaBroker = new EmbeddedKafkaBroker(1, true, 1);
        kafkaBroker.kafkaPorts(9092);
        kafkaBroker.zkPort(2181);
        kafkaBroker.zkConnectionTimeout(ZOOKEEPER_CONNECTION_TIMEOUT_MS);
        kafkaBroker.zkSessionTimeout(ZOOKEEPER_SESSION_TIMEOUT_MS);
        kafkaBroker.brokerProperty("log.dir", kafkaLogDirectory.getAbsolutePath());
        kafkaBroker.brokerProperty("log.cleaner.enable", "false");
        kafkaBroker.afterPropertiesSet();
        SpringApplication.run(ProceedApplication.class, args);
    }

    private static File createKafkaLogDirectory() {
        File kafkaLogRoot = new File(System.getProperty("user.dir"), "runtime/kafka");
        File kafkaLogDirectory = new File(kafkaLogRoot, "broker-" + UUID.randomUUID());
        if (!kafkaLogDirectory.mkdirs() && !kafkaLogDirectory.isDirectory()) {
            throw new IllegalStateException(
                    "Failed to create embedded Kafka log directory: " + kafkaLogDirectory
            );
        }
        return kafkaLogDirectory;
    }
}
