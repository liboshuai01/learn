package cn.liboshuai.flink;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

public class Mail implements Comparable<Mail> {

    // 全局递增序列号，用于保证相同优先级邮件的提交顺序（FIFO）
    private static final AtomicLong SEQ_COUNTER = new AtomicLong();

    // 真正的业务逻辑（例如：执行 Checkpoint，或者处理一条数据）
    private final ThrowingRunnable<? extends Exception> runnable;

    // 优先级（数字越小优先级越高）
    @Getter
    private final int priority;

    private final String description;

    private final long seqNum;

    public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String description) {
        this.runnable = runnable;
        this.priority = priority;
        this.description = description;
        this.seqNum = SEQ_COUNTER.getAndIncrement();
    }

    public void run() throws Exception {
        runnable.run();
    }

    @Override
    public String toString() {
        return description + " (priority=" + priority + ", seq=" + seqNum + ")";
    }

    @Override
    public int compareTo(Mail other) {
        int priorityCompare = Integer.compare(this.priority, other.priority);
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        return Long.compare(this.seqNum, other.seqNum);
    }
}
