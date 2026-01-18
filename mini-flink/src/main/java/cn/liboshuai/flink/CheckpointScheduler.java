package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class CheckpointScheduler extends Thread {
    private final MailboxExecutor taskMailboxExecutor;
    private final StreamTask task;
    private volatile boolean running = true;

    public CheckpointScheduler(StreamTask task) {
        super("Checkpoint-Timer");
        this.task = task;
        // 获取高优先级的执行器（Checkpoint 优先级 > 数据处理）
        this.taskMailboxExecutor = task.getControlMailboxExecutor();
    }

    @Override
    public void run() {
        long checkpointId = 0;
        while (running) {
            try {
                TimeUnit.MILLISECONDS.sleep(2000); // 每2秒触发一次
                long id = ++checkpointId;
                log.info("[JM] 触发 Checkpoint {}", id);
                taskMailboxExecutor.execute(() -> task.performCheckpoint(id), "Checkpoint-" + id);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
