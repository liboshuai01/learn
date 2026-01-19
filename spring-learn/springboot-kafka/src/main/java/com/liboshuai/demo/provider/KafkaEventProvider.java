package com.liboshuai.demo.provider;

import com.liboshuai.demo.dto.KafkaEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.concurrent.ListenableFutureCallback; // Spring 4.x/5.x
// import java.util.concurrent.CompletableFuture; // Spring 6.x / Spring Boot 3.x KafkaTemplate.send返回CompletableFuture

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class KafkaEventProvider {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${demo.kafka.provider-topic}")
    private String providerTopic;

    /**
     * 单条发送事件信息到Kafka (异步带回调)
     * @param event Kafka事件对象
     */
    public void sendEvent(KafkaEventDTO event) {
        if (event == null) {
            log.warn("尝试发送的KafkaEventDTO为null");
            return;
        }
        // 异步发送带回调
        kafkaTemplate.send(providerTopic, event.getName(), event) // 使用name作为Key，有助于分区策略
                .addCallback(new ListenableFutureCallback<SendResult<String, Object>>() { // For Spring Boot 2.x
                    @Override
                    public void onSuccess(SendResult<String, Object> result) {
                        // 此处的回调会覆盖 KafkaTemplate 全局配置的 ProducerListener
                        // 但通常全局的 ProducerListener 用于通用日志记录，这里可以做特定业务的成功处理
                        log.info("单个事件发送成功: Key={}, Offset={}",
                                result.getProducerRecord().key(), result.getRecordMetadata().offset());
                    }

                    @Override
                    public void onFailure(Throwable ex) {
                        log.error("单个事件发送失败: Key={}, Value={}, Error: {}",
                                event.getName(), event, ex.getMessage(), ex);
                        // 可以在此处实现特定于此发送操作的失败重试或告警逻辑
                    }
                });

        // 对于 Spring Boot 3.x / Spring 6.x, KafkaTemplate.send 返回 CompletableFuture
        /*
        kafkaTemplate.send(providerTopic, event.getName(), event)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("单个事件发送成功(CompletableFuture): Key={}, Offset={}",
                            result.getProducerRecord().key(), result.getRecordMetadata().offset());
                } else {
                    log.error("单个事件发送失败(CompletableFuture): Key={}, Value={}, Error: {}",
                            event.getName(), event, ex.getMessage(), ex);
                }
            });
        */
    }


    /**
     * 批量发送事件信息到Kafka
     * @param kafkaEventDTOList 事件列表
     */
    public void batchSend(List<KafkaEventDTO> kafkaEventDTOList) {
        if (CollectionUtils.isEmpty(kafkaEventDTOList)) {
            return;
        }
        log.info("准备批量发送 {} 条事件到主题 {}", kafkaEventDTOList.size(), providerTopic);
        for (KafkaEventDTO event : kafkaEventDTOList) {
            // 逐条发送，KafkaTemplate内部会根据batch-size和linger.ms进行批处理
            // 注意：如果对整个批次的原子性有要求（要么都成功，要么都失败），需要使用Kafka事务
            kafkaTemplate.send(providerTopic, event.getName(), event);
            // 如果希望每条消息都有独立的回调，则循环内使用 sendEvent 方法的异步回调逻辑
            // 但这可能会创建大量回调对象，对于纯粹的批量发送，依赖全局ProducerListener通常足够
        }
        // KafkaTemplate.flush(); // 如果需要立即发送，可以调用flush，但不建议频繁调用
        log.info("已提交 {} 条事件到Kafka发送队列", kafkaEventDTOList.size());
    }
}