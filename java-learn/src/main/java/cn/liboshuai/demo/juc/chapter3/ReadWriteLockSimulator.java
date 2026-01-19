package cn.liboshuai.demo.juc.chapter3;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 实战案例 4：ReentrantReadWriteLock 模拟 Flink 配置中心
 * 场景：
 * - ConfigRegistry: 存储全局配置 (Key-Value)。
 * - TaskThreads (Readers): 只有 5 个线程, 但每个线程都在疯狂读取配置。
 * - AdminThread (Writer): 只有 1 个线程, 偶尔(每秒)修改一次配置。
 * 核心目的：
 * 对比 ReentrantReadWriteLock 和 ReentrantLock (排他锁) 在读多写少场景下的性能差异。
 * 实验步骤：
 * 1. 默认使用 USE_RW_LOCK = true (读写锁)。观察控制台输出的 "读取速度"。
 * 2. 将 USE_RW_LOCK 改为 false (排他锁)。再次运行, 观察 "读取速度"。
 * 预期结果：
 * 使用读写锁时, 读取速度应该远快于排他锁, 且多个 Reader 线程几乎是同时在打印日志。
 */
public class ReadWriteLockSimulator {

    // ！！！开关：切换锁类型！！！
    // true: 使用 ReentrantReadWriteLock (高效)
    // false: 使用 ReentrantLock (低效)
    private static final boolean USE_RW_LOCK = true;

    static class ConfigRegistry {
        private final Map<String, String> configMap = new HashMap<>();

        // 读写锁
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Lock readLock = rwLock.readLock();
        private final Lock writeLock = rwLock.writeLock();

        // 普通排他锁 (用于对比)
        private final ReentrantLock exclusiveLock = new ReentrantLock();

        public ConfigRegistry() {
            configMap.put("db.host", "127.0.0.1");
            configMap.put("timeout", "5000");
        }

        public String getConfig(String key) {
            Lock lock = USE_RW_LOCK ? readLock : exclusiveLock;
            lock.lock();
            try {
                // 模拟读取耗时 (例如解析配置、计算哈希等)
                // 在读锁模式下, 多个线程可以同时进入这里 sleep, 所以吞吐量高
                try { Thread.sleep(50); } catch (InterruptedException e) { }

                return configMap.get(key);
            } finally {
                lock.unlock();
            }
        }

        public void updateConfig(String key, String value) {
            Lock lock = USE_RW_LOCK ? writeLock : exclusiveLock;
            lock.lock();
            try {
                // 模拟写入耗时
                try { Thread.sleep(50); } catch (InterruptedException e) { }

                System.out.println(">>> [Admin] 更新配置: " + key + " = " + value);
                configMap.put(key, value);
            } finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        ConfigRegistry registry = new ConfigRegistry();
        System.out.println("开始模拟... 使用锁模式: " + (USE_RW_LOCK ? "读写锁 (ReadWriteLock)" : "排他锁 (ReentrantLock)"));

        // 1. 启动 5 个 Reader 线程 (模拟 Task)
        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                while (true) {
                    long start = System.currentTimeMillis();
                    String val = registry.getConfig("db.host");
                    long end = System.currentTimeMillis();

                    System.out.println(Thread.currentThread().getName() + " 读取耗时: " + (end - start) + "ms, 值: " + val);

                    // 稍微歇一下，防止控制台刷屏太快
                    try { Thread.sleep(10); } catch (InterruptedException e) { }
                }
            }, "Reader-" + i).start();
        }

        // 2. 启动 1 个 Writer 线程 (模拟 Admin)
        new Thread(() -> {
            int version = 0;
            while (true) {
                try {
                    Thread.sleep(2000); // 每 2 秒写一次
                } catch (InterruptedException e) { }

                registry.updateConfig("db.host", "192.168.1." + version++);
            }
        }, "Writer").start();
    }
}
