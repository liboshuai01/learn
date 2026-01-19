package cn.liboshuai.learn.springboot.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component("kafkaConsumerExceptionHandler") // Bean名称与@KafkaListener中指定的一致
public class KafkaConsumerExceptionHandler implements KafkaListenerErrorHandler {

    /**
     * 处理单个消息消费失败的情况（如果监听器不是批处理模式）
     */
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        log.error("Kafka消费单个消息时发生错误。消息内容: {}, 异常: {}", message, exception.getMessage(), exception);
        // 根据业务决定如何处理：
        // 1. return null: 偏移量会被提交（如果配置了手动提交，且在方法里未提交，这里的return null不会影响，取决于listener的ack逻辑）。
        //    通常意味着“跳过”这个坏消息。需要配合ErrorHandlingDeserializer，否则原始异常可能导致容器停止。
        // 2. throw exception: 异常会继续向上传播。
        //    - 如果配置了重试机制(RetryTemplate) 和 DeadLetterPublishingRecoverer, 消息可能会被重试或发送到DLQ。
        //    - 如果没有，可能会导致消费者线程停止或不断重试同一条消息（取决于容器配置）。
        // 在批量消费模式下，此方法通常不会被直接调用，除非解开批次并分别处理。
        return null; // 默认行为：记录日志，消息将被视为已处理（如为手动ack，需listener确认）
    }

    /**
     * 处理批量消息消费失败的情况（如果监听器是批处理模式）
     * 注意：当KafkaListenerErrorHandler用于批量监听器时，这个handleError重载方法会被调用。
     * ListenerExecutionFailedException中的cause通常是原始异常。
     * message.getPayload()会是List<ConsumerRecord<?, ?>>类型。
     */
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        log.error("Kafka批量消费消息时发生错误。原始消息: {}, 异常: {}", message.getPayload(), exception.getMessage(), exception);

        // 对于批量消费的错误处理，策略更为复杂：
        // 1. **记录并跳过整个批次**：简单粗暴，但可能丢失数据。
        //    ack.acknowledge() // 如果在监听器中传递了Acknowledgment，可以在这里提交整个批次的offset
        //    return null;

        // 2. **尝试定位坏消息并分别处理/记录**：
        //    Object payload = message.getPayload();
        //    if (payload instanceof List) {
        //        List<ConsumerRecord<?, ?>> records = (List<ConsumerRecord<?, ?>>) payload;
        //        // 分析exception，可能它包含了导致失败的特定record的信息 (如果框架支持)
        //        // 或者，你可能需要逐条尝试处理，但这通常在Listener内部完成。
        //        // ErrorHandler通常用于对整个批次处理失败后的最终补救。
        //    }

        // 3. **不提交偏移量，让整个批次被重新消费**：
        //    这种情况通常是什么都不做，或者直接抛出异常，依赖于Kafka的Rebalance和重试机制。
        //    但这可能导致消息风暴，需要谨慎。

        // 4. **将整个批次或可疑消息发送到死信队列(DLQ)**：
        //    这是生产中常用的策略。

        // 默认行为：记录日志。对于手动ACK，如果监听器内部没有ack，则该批次消息不会被提交，下次会重新拉取。
        // 如果希望标记此批次处理完成（即使有错），则需要手动ack或确保监听器能ack。
        // Spring Kafka 2.8+ 引入了 BatchInterceptor 和 CommonErrorHandler 的改进，能更好地处理批量错误。
        // 对于更细致的批量错误处理 (例如，只提交成功的，将失败的发送到DLQ)，
        // 通常在 Listener 方法内部结合 Acknowledgment 来实现，或使用更高级的 ErrorHandler (如 SeekToCurrentBatchErrorHandler)。

        // 此处示例为简单记录日志，并依赖于Listener的ACK行为（或没有ACK导致重试）。
        return null;
    }
}
