package cn.liboshuai.learn.springboot.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cn.liboshuai.learn.springboot.kafka.dto.KafkaEventDTO; // 确保引入DTO
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload; // 可选，用于更清晰地绑定payload
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@Component
public class KafkaEventListener {

    @Resource
    private ObjectMapper objectMapper; // Spring Boot自动配置，用于手动解析JSON（如果需要）

    @KafkaListener(
            topics = "${demo.kafka.consumer-topic}",
            groupId = "${spring.kafka.consumer.group-id}", // 可以覆盖全局配置，但通常保持一致
            containerFactory = "kafkaListenerContainerFactory", // 指定使用哪个ListenerContainerFactory
            errorHandler = "kafkaConsumerExceptionHandler"      // 指定自定义的错误处理器
    )
    public void onEvents(@Payload List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.info("接收到批量消息，数量: {}", records.size());
        int processedCount = 0;
        for (ConsumerRecord<String, String> record : records) {
            try {
                // 详细打印消费的原始信息
                log.info("开始处理消息 - Topic: {}, Partition: {}, Offset: {}, Key: {}, Timestamp: {}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.timestamp());

                // 反序列化消息 Value
                // ErrorHandlingDeserializer已经尝试反序列化，如果失败，value会是null或特定错误对象
                // JsonDeserializer会尝试将String转为Object，但这里我们收到的是String，需手动转
                String jsonValue = record.value();
                if (jsonValue == null) {
                    log.warn("消息体为null (可能反序列化失败或原始消息为null), Offset: {}", record.offset());
                    // 根据业务逻辑决定是否跳过此消息或做其他处理
                    // 如果ErrorHandlingDeserializer已介入，这里可能是它生成的错误指示
                    // 如果希望即使部分失败也提交已成功的，需要更复杂的ACK策略(如BatchAcknowledgingMessageListener)
                    continue; // 跳过此条坏消息
                }

                KafkaEventDTO event = objectMapper.readValue(jsonValue, KafkaEventDTO.class);

                // ---- 业务逻辑处理 ----
                log.info("成功解析并处理事件: Name={}, Age={}, Sex={}, Timestamp={}",
                        event.getName(), event.getAge(), event.getSex(), event.getTimestamp());
                // 模拟业务处理耗时
                // Thread.sleep(50);
                // ---- 业务逻辑处理结束 ----
                processedCount++;

            } catch (JsonProcessingException e) {
                log.error("JSON反序列化失败 - Offset: {}, Key: {}, Value: '{}', 错误: {}",
                        record.offset(), record.key(), record.value(), e.getMessage());
                // 此处可选择：
                // 1. 记录错误，继续处理下一条（当前循环的continue）
                // 2. 如果希望此错误导致整个批次不被ACK，可以设置一个标志位，最后不调用ack.acknowledge()
                // 3. 或者将坏消息发送到死信队列 (DLQ)
                // 当前选择：记录日志，继续处理批次中的其他消息
            } catch (Exception e) {
                log.error("处理消息时发生未知业务异常 - Offset: {}, Key: {}, Value: '{}', 错误: {}",
                        record.offset(), record.key(), record.value(), e.getMessage(), e);
                // 类似地，根据业务需求决定如何处理
            }
        }

        // 所有消息处理（或尝试处理）完毕后，手动提交整个批次的偏移量
        if (!records.isEmpty()) { // 只有当批次非空时才提交
            try {
                ack.acknowledge();
                log.info("批处理完成，成功处理 {} 条消息，已提交Offset。", processedCount);
            } catch (Exception e) {
                log.error("手动提交Offset失败: {}", e.getMessage(), e);
                // 提交失败，意味着这批消息可能会被重新消费，需要监控和告警
            }
        } else {
            log.info("接收到空消息批次，无需提交Offset。");
        }
    }
}