package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo 11: 综合实战演练 - 选择正确的 API
 * 场景: 模拟一个 Flink 的 JobManager / ResourceManager，它需要并发地管理：
 * 1. 运行中 Job 的状态 (Job State)。
 * 2. 每个 Job 对应的昂贵资源（例如 ClassLoader）。
 * 3. 每个 Job 的运行时指标（例如处理的记录数）。
 * 目标: 针对每种操作，选择最安全、最高效、语义最清晰的 ConcurrentHashMap API。
 * (环境: JDK 1.8)
 */
public class Demo11 {

    // Map 1: 存储 Job 的当前状态
    private final ConcurrentHashMap<String, JobState> jobStates = new ConcurrentHashMap<>();
    // Map 2: 缓存 Job 的昂贵资源 (ClassLoader)
    private final ConcurrentHashMap<String, JobClassLoader> jobClassLoaders = new ConcurrentHashMap<>();
    // Map 3: 存储 Job 的指标
    private final ConcurrentHashMap<String, JobMetrics> jobMetrics = new ConcurrentHashMap<>();


    // --- 模拟的 Job 管理器 ---

    // --- Main 方法：模拟并发执行 ---
    public static void main(String[] args) throws InterruptedException {
        Demo11 manager = new Demo11();
        ExecutorService pool = Executors.newFixedThreadPool(10);

        String job1 = "job-001";
        String job2 = "job-002";

        // 1. 启动 Job-1 (正常流程)
        pool.submit(() -> {
            manager.registerJob(job1); // S1: putIfAbsent
            manager.initializeJob(job1); // S2: replace
            // 模拟 job 变为 FAILED
            manager.jobStates.put(job1, JobState.FAILED);
            System.out.println("[MainSim] Job " + job1 + " 状态设为 FAILED");
        });

        // 2. 启动 Job-2
        pool.submit(() -> {
            manager.registerJob(job2); // S1
            manager.initializeJob(job2); // S2
            manager.jobStates.put(job2, JobState.RUNNING); // 模拟
            System.out.println("[MainSim] Job " + job2 + " 状态设为 RUNNING");
        });

        // 3. 多个线程并发获取 Job-2 的 ClassLoader
        for (int i = 0; i < 5; i++) {
            pool.submit(() -> {
                manager.getOrCreateClassLoader(job2); // S3: computeIfAbsent
            });
        }

        // 4. 多个线程并发更新 Job-2 的指标
        for (int i = 0; i < 10; i++) {
            final long count = i;
            pool.submit(() -> {
                manager.addProcessedRecords(job2, count); // S4: computeIfPresent
            });
        }

        // 等待所有初始化任务完成
        Thread.sleep(2000);

        // 5. 报告和清理
        pool.submit(() -> {
            manager.reportMetrics(); // S6: mappingCount
            manager.cleanupFailedJobs(); // S5: 安全迭代
            System.out.println("\n--- 清理后 ---");
            manager.reportMetrics();
        });

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * 场景 1: 注册一个新 Job (Create)
     * 需求: 只有当 Job ID 不存在时，才将其注册为 CREATED 状态。
     */
    public boolean registerJob(String jobId) {
        System.out.println("[S1] 尝试注册 Job: " + jobId);

        // --- 常见的坑 (Check-then-Act 竞态条件) ---
        // if (!jobStates.containsKey(jobId)) {
        //     // 危险! 在你检查后，put 之前，另一个线程可能已经 put 了！
        //     jobStates.put(jobId, JobState.CREATED);
        // }

        // --- 正确选择: putIfAbsent (Demo 2) ---
        // 它原子地完成了 "如果不存在，就放入"
        JobState oldState = jobStates.putIfAbsent(jobId, JobState.CREATED);

        if (oldState == null) {
            System.out.println("[S1] 注册成功 (Job " + jobId + " -> CREATED)");
            return true;
        } else {
            System.out.println("[S1] 注册失败 (Job " + jobId + " 已存在，状态为 " + oldState + ")");
            return false;
        }
    }

    /**
     * 场景 2: 状态转换 (Compare-and-Swap, CAS)
     * 需求: 只有当 Job "当前" 处于 CREATED 状态时，才将其原子地变为 INITIALIZING。
     */
    public boolean initializeJob(String jobId) {
        System.out.println("[S2] 尝试初始化 Job: " + jobId);

        // --- 常见的坑 (Get-then-Put 竞态条件) ---
        // JobState currentState = jobStates.get(jobId);
        // if (currentState == JobState.CREATED) {
        //     // 危险! 在你 get 和 put 之间，另一个线程可能已将其变为 FAILED！
        //     jobStates.put(jobId, JobState.INITIALIZING);
        // }

        // --- 正确选择: replace(key, oldV, newV) (Demo 3) ---
        // 它原子地完成了 "比较并替换"
        boolean success = jobStates.replace(jobId, JobState.CREATED, JobState.INITIALIZING);

        if (success) {
            System.out.println("[S2] 初始化成功 (Job " + jobId + " -> INITIALIZING)");
            return true;
        } else {
            System.out.println("[S2] 初始化失败 (Job " + jobId + " 状态不是 CREATED)");
            return false;
        }
    }

    /**
     * 场景 3: 获取或创建昂贵资源 (Get-or-Create / Cache)
     * 需求: 获取 Job 的 ClassLoader。如果缓存中没有，则创建它，
     * 并确保在并发时只创建一个实例。
     */
    public JobClassLoader getOrCreateClassLoader(String jobId) {
        System.out.println("[S3] 尝试获取 ClassLoader: " + jobId);

        // --- 常见的坑 (Check-then-Act) ---
        // JobClassLoader cl = jobClassLoaders.get(jobId);
        // if (cl == null) {
        //     cl = new JobClassLoader(jobId); // 昂贵操作
        //     // 危险! 两个线程可能都执行到这里，创建两个 ClassLoader
        //     jobClassLoaders.put(jobId, cl);
        // }
        // return cl;

        // --- 正确选择: computeIfAbsent (Demo 5) ---
        // Flink 源码中最常见的模式！
        // "计算函数" (key -> new JobClassLoader(key))
        // 只会在 key 不存在时被调用，并且是在锁的保护下调用的。
        JobClassLoader loader = jobClassLoaders.computeIfAbsent(
                jobId,
                key -> new JobClassLoader(key) // 昂贵的创建函数 (mappingFunction)
        );

        System.out.println("[S3] 成功获取 ClassLoader: " + jobId);
        return loader;
    }

    /**
     * 场景 4: 原子地更新指标 (Read-Modify-Write)
     * 需求: 为一个 "已存在" 的 Job 增加处理的记录数。
     */
    public void addProcessedRecords(String jobId, long count) {
        // --- 场景 4.1: 如果 Map 中还没有 JobMetrics，先创建它 ---
        // 我们需要先确保指标对象存在 (使用 computeIfAbsent)
        // (在真实场景中，这可能在 registerJob 时就已创建)
        jobMetrics.computeIfAbsent(jobId, k -> new JobMetrics());

        // --- 场景 4.2: 原子地更新这个已存在的对象 ---
        // --- 正确选择: computeIfPresent (Demo 6) ---
        // (k, metrics) -> { ... return metrics; }
        // 我们只在 "key 存在" 时执行操作 (更新 metrics
        // 对象的内部状态)，并返回修改后的对象。
        jobMetrics.computeIfPresent(jobId, (key, metrics) -> {
            metrics.addRecords(count); // 修改对象内部
            System.out.println("[S4] 更新指标 (Job " + jobId + "): +" + count +
                    ", 总计: " + metrics.getProcessedRecords());
            return metrics; // 必须返回被修改的对象
        });

        // --- 备选方案: compute (Demo 4) ---
        // compute 也能实现，但更繁琐，因为它需要处理 value == null 的情况
        // jobMetrics.compute(jobId, (key, metrics) -> {
        //     if (metrics == null) metrics = new JobMetrics();
        //     metrics.addRecords(count);
        //     return metrics;
        // });

        // --- 备选方案: merge (Demo 7) ---
        // merge 更适合 "聚合"，比如 (oldValue, newValue) -> sum
        // 我们的场景是 "修改" 一个已存在的对象状态，用 computeIfPresent 语义更清晰。
    }

    /**
     * 场景 5: 安全迭代与清理
     * 需求: 遍历所有 Job，移除所有 "FAILED" 状态的 Job。
     */
    public void cleanupFailedJobs() {
        System.out.println("[S5] 开始清理 FAILED Jobs...");

        // --- 正确选择: 迭代器 或 keySet 遍历 (Demo 8) ---
        // ConcurrentHashMap 的迭代器是安全的，不会抛出
        // ConcurrentModificationException

        // 方式 A: 使用 keySet 和 map.remove
        // (这是 Flink 源码中更常见的模式)
        for (String jobId : jobStates.keySet()) {
            // (注意: 这里的 get 也是线程安全的)
            if (jobStates.get(jobId) == JobState.FAILED) {
                // remove 是线程安全的
                if (jobStates.remove(jobId, JobState.FAILED)) {
                    System.out.println("[S5] 清理 FAILED Job: " + jobId);
                    // 同时清理其他 Map
                    jobClassLoaders.remove(jobId);
                    jobMetrics.remove(jobId);
                }
            }
        }

        // 方式 B: 使用迭代器 (也是安全的)
        // Iterator<Map.Entry<String, JobState>> iterator = jobStates.entrySet().iterator();
        // while (iterator.hasNext()) {
        //     Map.Entry<String, JobState> entry = iterator.next();
        //     if (entry.getValue() == JobState.FAILED) {
        //         System.out.println("[S5] 清理 FAILED Job: " + entry.getKey());
        //         iterator.remove(); // 安全
        //     }
        // }
    }

    /**
     * 场景 6: 获取报告 (估算大小)
     * 需求: 报告当前有多少个 Job 在追踪。
     */
    public void reportMetrics() {
        // --- 常见的坑 ---
        // int size = jobStates.size();
        // if (size == 0) { ... } // 危险! size() 是估算值，可能在并发时返回 0

        // --- 正确选择: isEmpty() (Demo 10) ---
        if (jobStates.isEmpty()) {
            System.out.println("[S6] 报告: 当前没有 Job。");
            return;
        }

        // --- 正确选择: mappingCount() (Demo 10) ---
        // 返回 long，且明确了是 "估算值"
        long count = jobStates.mappingCount();
        System.out.println("[S6] 报告: 当前约有 " + count + " 个 Job。");
        System.out.println("   - 状态: " + jobStates);
        System.out.println("   - 指标: " + jobMetrics);
    }

    // 1. Job 状态枚举
    enum JobState {
        CREATED,
        INITIALIZING,
        RUNNING,
        FAILED,
        FINISHED
    }

    // 2. 模拟的昂贵资源
    static class JobClassLoader {
        private final String jobId;

        public JobClassLoader(String jobId) {
            this.jobId = jobId;
            // 模拟昂贵的创建过程
            System.out.println("!!! [Thread: " + Thread.currentThread().getName() +
                    "] 正在执行昂贵操作：为 " + jobId + " 创建 ClassLoader...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }

        public String getJobId() {
            return jobId;
        }
    }

    // 3. Job 指标 (一个可变对象)
    static class JobMetrics {
        private final AtomicLong processedRecords = new AtomicLong(0);

        public void addRecords(long count) {
            this.processedRecords.addAndGet(count);
        }

        public long getProcessedRecords() {
            return processedRecords.get();
        }

        @Override
        public String toString() {
            return "JobMetrics{processedRecords=" + processedRecords + '}';
        }
    }
}
