package cn.liboshuai.demo.juc.chapter5;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 实战案例 6：ConcurrentHashMap 的 "原子性陷阱"
 * 目标：
 * 演示为什么 "get() + put()" 在 ConcurrentHashMap 中是线程不安全的 (非原子复合操作),
 * 以及如何使用 "compute()" 来正确地执行原子复合操作。
 * 场景：
 * 模拟 Flink JobMaster 统计 FAILED 的 Task 数量。
 * 多个 RPC 线程并发地汇报 "task-failed" 事件。
 * 我们需要在一个 Map 中累加每个 Job 的失败次数。
 */
public class ConcurrentHashMapSimulator {

    private final Map<String, Long> failureCounts = new ConcurrentHashMap<>();
    private final String JOB_ID = "job_12345";

    /**
     * ！！！ 1. 反面教材 (线程不安全) ！！！
     * "先检查, 再行动" (Check-Then-Act)
     * 竞争过程：
     * 1. 线程 A: get("job_12345") -> 得到 5
     * 2. 线程 B: get("job_12345") -> 得到 5
     * 3. 线程 A: newCount = 5 + 1 = 6
     * 4. 线程 A: put("job_12345", 6)
     * 5. 线程 B: newCount = 5 + 1 = 6
     * 6. 线程 B: put("job_12345", 6)  <-- 线程 A 的更新丢失了!
     */
    public void recordFailureUnsafe() {
        // 1. Get (Check)
        Long currentCount = failureCounts.get(JOB_ID);

        if (currentCount == null) {
            // 2. Put (Act)
            failureCounts.put(JOB_ID, 1L);
        } else {
            // 2. Put (Act)
            failureCounts.put(JOB_ID, currentCount + 1);
        }
    }

    /**
     * ！！！ 2. JUC/Flink 推荐 (原子操作) ！！！
     * 使用 'compute()' 方法。
     * CHM 会保证 'compute' 内部的 lambda 表达式
     * 是在一个 "锁" (通常是该槽位的头节点) 保护下执行的。
     */
    public void recordFailureSafe() {
        // 'compute' 会原子地执行:
        // 1. 获取 key 对应的 value (或 null)
        // 2. 执行 lambda 表达式 (key, oldValue) -> newValue
        // 3. 将 newValue 存回
        // 整个过程是原子的
        failureCounts.compute(JOB_ID, (key, oldValue) -> {
            if (oldValue == null) {
                return 1L;
            } else {
                return oldValue + 1L;
            }
        });

        // 也可以用 'merge' 方法, 语义更简洁:
        // failureCounts.merge(JOB_ID, 1L, Long::sum);
    }

    // ----- 模拟器主程序 -----
    public static void main(String[] args) throws InterruptedException {
        int threads = 10;
        int incrementsPerThread = 1_000_000;
        long expectedTotal = (long) threads * incrementsPerThread;

        System.out.println("开始模拟 " + threads + " 个线程, 每个执行 " + incrementsPerThread + " 次 'inc()'");
        System.out.println("预期总数: " + expectedTotal);
        System.out.println("==============================================");

        // --- 实验 1: 运行不安全的累加器 ---
        System.out.println("--- 实验 1: UnsafeUpdater (get + put) ---");
        runSimulation(new UnsafeUpdater(), threads, incrementsPerThread, expectedTotal);

        System.out.println("==============================================");

        // --- 实验 2: 运行安全的累加器 ---
        System.out.println("--- 实验 2: SafeUpdater (compute) ---");
        runSimulation(new SafeUpdater(), threads, incrementsPerThread, expectedTotal);
    }

    // --- 辅助运行的内部类 ---
    interface Updater {
        void update(ConcurrentHashMapSimulator simulator);
        long getResult(ConcurrentHashMapSimulator simulator, String jobId);
    }

    static class UnsafeUpdater implements Updater {
        @Override
        public void update(ConcurrentHashMapSimulator simulator) {
            simulator.recordFailureUnsafe();
        }
        @Override
        public long getResult(ConcurrentHashMapSimulator simulator, String jobId) {
            return simulator.failureCounts.getOrDefault(jobId, 0L);
        }
    }

    static class SafeUpdater implements Updater {
        @Override
        public void update(ConcurrentHashMapSimulator simulator) {
            simulator.recordFailureSafe();
        }
        @Override
        public long getResult(ConcurrentHashMapSimulator simulator, String jobId) {
            return simulator.failureCounts.getOrDefault(jobId, 0L);
        }
    }

    private static void runSimulation(Updater updater, int threads, int incrementsPerThread, long expectedTotal)
            throws InterruptedException {

        ConcurrentHashMapSimulator simulator = new ConcurrentHashMapSimulator();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        long startTime = System.nanoTime();

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    updater.update(simulator);
                }
            });
        }

        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        long endTime = System.nanoTime();
        long finalCount = updater.getResult(simulator, simulator.JOB_ID);

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
