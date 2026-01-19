package cn.liboshuai.learn.springboot.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer; // 使用JSON反序列化

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.auto-commit-interval}")
    private String autoCommitInterval; // 注意：若enableAutoCommit=false，此参数无效
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;
    @Value("${spring.kafka.consumer.properties.session.timeout.ms}")
    private String sessionTimeoutMs; // YAML中路径已更新
    @Value("${spring.kafka.consumer.properties.max.poll.interval.ms}")
    private String maxPollIntervalMs; // YAML中路径已更新
    @Value("${spring.kafka.consumer.max-poll-records}")
    private String maxPollRecords;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.listener.concurrency}")
    private Integer concurrency;
    @Value("${spring.kafka.listener.missing-topics-fatal}")
    private boolean missingTopicsFatal;
    @Value("${spring.kafka.listener.poll-timeout}")
    private long pollTimeout;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>(16);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        if (enableAutoCommit) { // 仅当自动提交开启时，此参数才有意义
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        // props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000"); // 一般为session.timeout.ms / 3

        // KEY 反序列化器: ErrorHandlingDeserializer包装StringDeserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);

        // VALUE 反序列化器: ErrorHandlingDeserializer包装StringDeserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        // 配置JsonDeserializer信任所有包路径，或指定特定包路径以增强安全性
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        // 如果发送端JsonSerializer未添加类型信息，或希望反序列化为特定类型，可以配置
        // props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.example.yourpackage.YourDtoClass");
        // props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false"); //如果发送端未加type header，则需要设为false

        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() { // Key类型为String
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean("kafkaListenerContainerFactory") // Bean名称与@KafkaListener中指定的一致
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) { // 注入ConsumerFactory
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.setMissingTopicsFatal(missingTopicsFatal);
        factory.getContainerProperties().setPollTimeout(pollTimeout);

        // 手动提交ACK模式
        if (!enableAutoCommit) {
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        }

        // 开启批量消费
        factory.setBatchListener(true); // 与 application.yml 中的 listener.type: batch 效果相同

        // 可选：配置通用错误处理器，例如记录到DLQ(死信队列)
        // factory.setCommonErrorHandler(new DefaultErrorHandler(...));

        return factory;
    }
}