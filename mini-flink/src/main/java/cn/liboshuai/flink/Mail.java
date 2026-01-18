package cn.liboshuai.flink;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 封装了具体的任务（Runnable）和优先级。
 * 修改点：实现了 Comparable 接口，并增加了 seqNum 以保证同优先级的 FIFO 顺序。
 */
public class Mail implements Comparable<Mail> {

    // 全局递增序列号，用于保证相同优先级邮件的提交顺序 (FIFO)
    private static final AtomicLong SEQ_COUNTER = new AtomicLong();

    // 真正的业务逻辑 (例如：执行 Checkpoint，或者处理一条数据)
    // 注意：这里假设 ThrowingRunnable 定义在包级别或 MiniFlink 中
    private final ThrowingRunnable<? extends Exception> runnable;

    // 优先级 (数字越小优先级越高)
    @Getter
    private final int priority;

    // 描述信息，用于调试 (例如 "Checkpoint 15")
    private final String description;

    // 序列号：在创建时生成，用于解决 PriorityQueue 不稳定排序的问题
    private final long seqNum;

    public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String description) {
        this.runnable = runnable;
        this.priority = priority;
        this.description = description;
        // 获取当前唯一递增序号
        this.seqNum = SEQ_COUNTER.getAndIncrement();
    }

    /**
     * 执行邮件中的逻辑
     */
    public void run() throws Exception {
        runnable.run();
    }

    @Override
    public String toString() {
        return description + " (priority=" + priority + ", seq=" + seqNum + ")";
    }

    /**
     * 优先级比较核心逻辑 (仿照 Flink 1.18)
     * 1. 优先比较 priority (数值越小，优先级越高)
     * 2. 如果 priority 相同，比较 seqNum (数值越小，提交越早，越先执行)
     */
    @Override
    public int compareTo(Mail other) {
        int priorityCompare = Integer.compare(this.priority, other.priority);
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        // 优先级相同，严格按照 FIFO
        return Long.compare(this.seqNum, other.seqNum);
    }
}
