package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CounterStreamTask extends StreamTask implements DataOutput {

    private final StreamInputProcessor inputProcessor;
    private long recordCount = 0;

    public CounterStreamTask(MiniInputGate inputGate) {
        super();
        this.inputProcessor = new StreamInputProcessor(inputGate, this);
        // 注册第一个定时器：1 秒后触发
        registerPeriodicTimer(1000);
    }

    /**
     * 注册周期性定时器的演示方法
     */
    private void registerPeriodicTimer(int delayMs) {
        long triggerTime = timerService.getCurrentProcessingTime() + delayMs;
        // 1. 向 TimerService 注册（这是在后台线程池触发）
        timerService.registerTimer(triggerTime, timestamp -> {
            // 2. [关键] 定时器触发，我们身处于 "Flink-System-Timer-Service" 线程
            // 绝对不能直接操作 recordCount 等状态！
            // 必须通过 mailboxExecutor 将逻辑“邮寄”回主线程执行
            mainMailboxExecutor.execute(() -> {
                // 这里是主线程，安全地访问状态
                onTimer(timestamp);
            }, "PeriodicTimer-" + timestamp);
        });
    }

    /**
     * 定时器具体的业务逻辑（运行在主线程）
     */
    private void onTimer(long timestamp) {
        log.info(" >>> [Timer Fired] timestamp: {}, 当前处理条数: {}", timestamp, recordCount);
        // 注册下一个定时器（模拟周期性）
        registerPeriodicTimer(1000);
    }

    @Override
    public void processRecord(String record) {
        this.recordCount++;
        if (recordCount % 10 == 0) {
            log.info("Task 处理进度：{} 条", recordCount);
        }
    }

    @Override
    public void runDefaultAction(Controller controller) throws Exception {
        inputProcessor.runDefaultAction(controller);
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
