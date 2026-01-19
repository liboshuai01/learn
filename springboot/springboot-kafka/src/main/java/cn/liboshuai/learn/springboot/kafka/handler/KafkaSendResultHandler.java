package cn.liboshuai.learn.springboot.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component("kafkaSendResultHandler") // Bean名称与ProducerConfig中注入的一致
public class KafkaSendResultHandler implements ProducerListener<String, Object> {

    @Override
    public void onSuccess(ProducerRecord<String, Object> producerRecord, RecordMetadata recordMetadata) {
        // 生产实践中，可根据需求选择性记录日志，避免日志过多
        // if (log.isDebugEnabled()) { // 例如，只在Debug级别记录详细成功信息
        log.info("Kafka消息发送成功 -> 主题: {}, 分区: {}, 偏移量: {}, Key: {}, Value类型: {}",
                producerRecord.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                producerRecord.key(),
                producerRecord.value() != null ? producerRecord.value().getClass().getName() : "null");
        // }
        // 可在此处添加监控指标上报等逻辑
    }

    @Override
    public void onError(ProducerRecord<String, Object> producerRecord, RecordMetadata recordMetadata, Exception exception) {
        // recordMetadata 可能为 null，如果错误发生在发送到broker之前
        String topic = producerRecord.topic();
        Object key = producerRecord.key();
        Object value = producerRecord.value();
        String valueType = value != null ? value.getClass().getName() : "null";

        if (recordMetadata != null) {
            log.error("Kafka消息发送失败! 主题: {}, 分区: {}, 偏移量: {}, Key: {}, Value类型: {}, 异常: {}",
                    topic,
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    key,
                    valueType,
                    exception.getMessage(), exception);
        } else {
            log.error("Kafka消息发送失败! (RecordMetadata为null) 主题: {}, Key: {}, Value类型: {}, 异常: {}",
                    topic,
                    key,
                    valueType,
                    exception.getMessage(), exception);
        }
        // 生产实践中，发送失败的消息可考虑：
        // 1. 记录到数据库或专门的失败队列，后续进行重试或人工干预。
        // 2. 触发告警。
    }
}
