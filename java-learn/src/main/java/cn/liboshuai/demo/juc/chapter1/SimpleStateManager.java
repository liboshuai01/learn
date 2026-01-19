package cn.liboshuai.demo.juc.chapter1;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 实战案例 2：模拟 Flink BlobServer/CheckpointCoordinator 使用 synchronized
 * * 演示多个线程并发访问一个 "共享状态" (这里是一个 HashMap)。
 * * 思考：
 * 1. 如果把 'updateState' 和 'getState' 方法的 'synchronized' 关键字去掉会发生什么？
 * (提示：可能会有 ConcurrentModificationException, 或者数据丢失/覆盖)
 * 2. 为什么这里不用 volatile？
 * (提示：volatile 不保证原子性, 'get' + 'put' 是复合操作)
 * 3. 为什么不用 ReentrantLock？
 * (提示：可以，但 'synchronized' 更简单。这里没有 'ReentrantLock' 的高级需求)
 */
public class SimpleStateManager {

    /**
     * ！！！这就是 Flink 中的 "共享状态" ！！！
     * (例如 BlobServer 里的 'jobBLOBs' Map)
     * 我们特意使用非线程安全的 HashMap 来演示问题。
     */
    private final Map<String, Integer> stateMap = new HashMap<>();

    /**
     * ！！！使用 'synchronized' 保护 "写" 操作！！！
     * 这是一个 "Check-Then-Act" 复合操作, 必须是原子的。
     */
    public synchronized void updateState(String key) {
        // 1. 检查 (Check)
        Integer oldValue = stateMap.get(key);
        if (oldValue == null) {
            // 2. 行动 (Act)
            stateMap.put(key, 1);
        } else {
            // 2. 行动 (Act)
            stateMap.put(key, oldValue + 1);
        }
        System.out.println(Thread.currentThread().getName() + " 更新了 " + key + ", new value=" + stateMap.get(key));
    }

    /**
     * ！！！使用 'synchronized' 保护 "读" 操作！！！
     * 保证读取时, 'updateState' 方法不会同时在修改 Map。
     */
    public synchronized Integer getState(String key) {
        return stateMap.get(key);
    }

    /**
     * 返回 Map 的大小, 也需要同步
     */
    public synchronized int getStateSize() {
        return stateMap.size();
    }


    public static void main(String[] args) throws InterruptedException {
        // 1. 创建一个固定10个线程的线程池
        // 模拟 Flink 的多个 RPC 线程或管理线程
        ExecutorService executor = Executors.newFixedThreadPool(10);

        // 2. 创建 "共享" 的状态管理器实例
        // ！！！所有线程都将操作这同一个实例！！！
        SimpleStateManager stateManager = new SimpleStateManager();

        int numTasks = 1000;
        String key = "myJobID";

        System.out.println("开始并发更新... (10个线程, 每个线程更新1000次)");

        // 3. 提交 10 * 1000 = 10000 个并发任务
        for (int i = 0; i < numTasks * 10; i++) {
            executor.submit(() -> {
                // 模拟 Flink 中不同 Job 的并发请求
                // 为了演示简单, 我们这里只用一个 key
                // 你也可以改成 UUID.randomUUID().toString() 来模拟不同的 key
                stateManager.updateState(key);
            });
        }

        // 4. 关闭线程池并等待所有任务完成
        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
            System.err.println("线程池没有在10秒内结束");
            executor.shutdownNow();
        }

        // 5. 验证最终结果
        // 因为有 1000 * 10 = 10000 次更新
        // 所以 stateMap 中 "myJobID" 对应的值应该是 10000
        System.out.println("========================================");
        System.out.println("所有线程执行完毕。");
        System.out.println("预期结果: 10000");
        System.out.println("实际结果: " + stateManager.getState(key));

        // (可选) 尝试去掉 synchronized 关键字, 看看会发生什么
        // 你会发现实际结果远小于 10000, 甚至可能抛出异常
    }
}
