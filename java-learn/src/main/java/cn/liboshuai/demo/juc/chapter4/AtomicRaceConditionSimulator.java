package cn.liboshuai.demo.juc.chapter4;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实战案例 5：模拟 Flink Metrics 的原子计数器
 * 目标：
 * 亲眼见证 "i++" 在并发下的 "数据丢失" (Race Condition),
 * 以及 AtomicLong 如何解决这个问题。
 * 场景：
 * 10 个线程 (模拟 10 个并发 Task 或数据源)
 * 每个线程都对同一个计数器执行 1,000,000 (一百万) 次 'inc()' 操作。
 * 预期结果：
 * 10 * 1,000,000 = 10,000,000 (一千万)
 */
public class AtomicRaceConditionSimulator {

    // ----- 计数器接口 -----
    interface Counter {
        void inc();
        long get();
    }

    /**
     * 1. 线程不安全的计数器
     * ！！！反面教材！！！
     * 使用 'long++'
     */
    static class UnsafeCounter implements Counter {
        private long count = 0;

        @Override
        public void inc() {
            // 这不是一个原子操作
            // (Read -> Modify -> Write)
            count++;
        }

        @Override
        public long get() {
            return count;
        }
    }

    /**
     * 2. 线程安全的计数器
     * ！！！Flink 的选择！！！
     * 使用 'AtomicLong'
     */
    static class AtomicCounter implements Counter {
        // Flink Metrics 和 Buffer RefCnt 的同款选择
        private final AtomicLong count = new AtomicLong(0);

        @Override
        public void inc() {
            // 这是一个原子的 CAS 操作
            // CPU 级别保证 '加1' 的完整性
            count.incrementAndGet();
        }

        @Override
        public long get() {
            return count.get();
        }
    }

    // ----- 模拟器主程序 -----
    public static void main(String[] args) throws InterruptedException {
        int threads = 10;
        int incrementsPerThread = 1_000_000;
        long expectedTotal = (long) threads * incrementsPerThread;

        System.out.println("开始模拟 " + threads + " 个线程, 每个执行 " + incrementsPerThread + " 次 'inc()'");
        System.out.println("预期总数: " + expectedTotal);
        System.out.println("==============================================");

        // --- 实验 1: 运行不安全的计数器 ---
        System.out.println("--- 实验 1: UnsafeCounter (long++) ---");
        runSimulation(new UnsafeCounter(), threads, incrementsPerThread, expectedTotal);

        System.out.println("==============================================");

        // --- 实验 2: 运行安全的计数器 ---
        System.out.println("--- 实验 2: AtomicCounter (AtomicLong) ---");
        runSimulation(new AtomicCounter(), threads, incrementsPerThread, expectedTotal);
    }

    private static void runSimulation(Counter counter, int threads, int incrementsPerThread, long expectedTotal)
            throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        long startTime = System.nanoTime();

        // 提交所有任务
        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    counter.inc();
                }
            });
        }

        // 等待所有线程执行完毕
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            System.err.println("模拟超时!");
            executor.shutdownNow();
        }

        long endTime = System.nanoTime();
        long finalCount = counter.get();

        // 报告结果
        System.out.println("模拟结束。");
        System.out.println("耗时: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
        System.out.println("最终计数: " + finalCount);

        if (finalCount == expectedTotal) {
            System.out.println("结果: 正确 (✓)");
        } else {
            System.err.println("结果: 错误 (✗) !!!!!!!!!!! 数据丢失了 " + (expectedTotal - finalCount) + " !!!!!!!!!!!");
        }
    }
}
