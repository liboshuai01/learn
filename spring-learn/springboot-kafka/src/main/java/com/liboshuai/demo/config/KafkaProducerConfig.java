package com.liboshuai.demo.config;

import com.liboshuai.demo.handler.KafkaSendResultHandler; // 假设这是回调处理类
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer; // 使用JSON序列化

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaProducerConfig { // 类名修改为Producer以区分

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.retries}")
    private Integer retries; // 类型改为Integer
    @Value("${spring.kafka.producer.batch-size}")
    private Integer batchSize; // 类型改为Integer
    @Value("${spring.kafka.producer.buffer-memory}")
    private Long bufferMemory; // 类型改为Long
    // 可选地从配置文件读取 linger.ms, compression.type, enable.idempotence 等
    @Value("${spring.kafka.producer.properties.linger.ms:50}") // 默认50ms
    private Long lingerMs;
    @Value("${spring.kafka.producer.properties.compression.type:snappy}") // 默认snappy
    private String compressionType;
    @Value("${spring.kafka.producer.properties.enable.idempotence:true}") // 默认true
    private Boolean enableIdempotence;

    @Resource
    private KafkaSendResultHandler kafkaSendResultHandler;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>(16);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs); // 实际项目中建议配置
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType); // 根据需要启用压缩
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence); // 开启幂等性以实现EOS保障

        // Key和Value的序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 使用Spring Kafka提供的JsonSerializer，支持泛型对象
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // 如果Value是具体类型，可以配置JsonSerializer以移除类型信息，减小消息体积
        // props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return props;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(producerConfigs());
        // 如果启用了Kafka事务，在此处配置事务ID前缀
        // factory.setTransactionIdPrefix(transactionIdPrefix);
        return factory;
    }

    // Kafka事务管理器，仅在启用Kafka事务时需要
    // @Bean
    // public KafkaTransactionManager<String, Object> kafkaTransactionManager(ProducerFactory<String, Object> producerFactory) {
    //     return new KafkaTransactionManager<>(producerFactory);
    // }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) { // 注入ProducerFactory
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        // 设置发送结果回调监听器
        kafkaTemplate.setProducerListener(kafkaSendResultHandler);
        return kafkaTemplate;
    }
}