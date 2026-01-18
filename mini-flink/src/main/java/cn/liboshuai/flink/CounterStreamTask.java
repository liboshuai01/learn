package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

/**
 * 具体的业务 Task。
 * 修改点：
 * 1. 在构造函数中注册了一个周期性的定时器。
 * 2. 演示了标准的 Flink Mailbox 定时器模式：Timer线程 -> Mailbox -> Main线程。
 */
@Slf4j
public class CounterStreamTask extends StreamTask implements StreamInputProcessor.DataOutput {

    private final StreamInputProcessor inputProcessor;
    private long recordCount = 0;

    public CounterStreamTask(MiniInputGate inputGate) {
        super();
        this.inputProcessor = new StreamInputProcessor(inputGate, this);

        // [新增] 注册第一个定时器：1秒后触发
        registerPeriodicTimer(1000);
    }

    /**
     * [新增] 注册周期性定时器的演示方法
     */
    private void registerPeriodicTimer(long delayMs) {
        long triggerTime = timerService.getCurrentProcessingTime() + delayMs;

        // 1. 向 TimerService 注册 (这是在后台线程池触发)
        timerService.registerTimer(triggerTime, timestamp -> {

            // 2. [关键] 定时器触发时，我们身处 "Flink-System-Timer-Service" 线程。
            // 绝对不能直接操作 recordCount 等状态！
            // 必须通过 mailboxExecutor 将逻辑"邮寄"回主线程执行。
            mainMailboxExecutor.execute(() -> {
                // 这里是主线程，安全地访问状态
                onTimer(timestamp);
            }, "PeriodicTimer-" + timestamp);
        });
    }

    /**
     * [新增] 定时器具体的业务逻辑（运行在主线程）
     */
    private void onTimer(long timestamp) {
        log.info(" >>> [Timer Fired] timestamp: {}, 当前处理条数: {}", timestamp, recordCount);

        // 注册下一次定时器 (模拟周期性)
        registerPeriodicTimer(1000);
    }

    @Override
    public void runDefaultAction(Controller controller) {
        inputProcessor.runDefaultAction(controller);
    }

    @Override
    public void processRecord(String record) {
        this.recordCount++;
        if (recordCount % 10 == 0) {
            log.info("Task 处理进度: {} 条", recordCount);
        }
    }

    @Override
    public void performCheckpoint(long checkpointId) {
        log.info(" >>> [Checkpoint Starting] ID: {}, 当前状态值: {}", checkpointId, recordCount);
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info(" <<< [Checkpoint Finished] ID: {} 完成", checkpointId);
    }
}
