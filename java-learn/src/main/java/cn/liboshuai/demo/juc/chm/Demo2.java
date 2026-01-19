package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 2: ConcurrentHashMap.putIfAbsent 用法
 * 演示:
 * 1. putIfAbsent(K key, V value) 的原子性。
 * 2. 它只会 C (Create)，不会 U (Update)。
 * 3. 通过多线程模拟并发场景，看 putIfAbsent 如何保证只有一个线程能成功插入。
 * (环境: JDK 1.8)
 */
public class Demo2 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        // 1. 基本用法
        System.out.println("--- 1. 基本用法 ---");
        // 第一次尝试放入 "Flink"
        String oldValue1 = map.putIfAbsent("Flink", "Initial-Value-1");
        System.out.println("第一次放入 'Flink'，返回的旧值: " + oldValue1);
        System.out.println("Map 内容: " + map);

        // 第二次尝试放入 "Flink" (key 已存在)
        String oldValue2 = map.putIfAbsent("Flink", "New-Value-2");
        System.out.println("\n第二次放入 'Flink'，返回的旧值: " + oldValue2);
        System.out.println("Map 内容 (未改变): " + map);

        // 放入新的 key "Spark"
        String oldValue3 = map.putIfAbsent("Spark", "Spark-Value");
        System.out.println("\n放入 'Spark'，返回的旧值: " + oldValue3);
        System.out.println("Map 内容: " + map);

        System.out.println("\n--- 2. 并发场景演示 ---");
        // 模拟10个线程同时尝试为 "Job-1" 赋值
        // 只有一个线程能成功写入 "Job-1-Value-Thread-X"
        // 其他线程都会返回那个成功写入的值

        String key = "Job-1";
        ExecutorService pool = Executors.newFixedThreadPool(10);
        ConcurrentHashMap<String, String> concurrentMap = new ConcurrentHashMap<>();

        for (int i = 0; i < 10; i++) {
            final int threadNum = i;
            pool.submit(() -> {
                String valueToPut = "Job-1-Value-Thread-" + threadNum;
                // 核心：原子操作
                String existingValue = concurrentMap.putIfAbsent(key, valueToPut);

                if (existingValue == null) {
                    // existingValue 为 null，说明当前线程是第一个成功放入的
                    System.out.println("线程 " + threadNum + " 成功放入值: " + valueToPut);
                } else {
                    // existingValue 不为 null，说明 key 已经存在
                    System.out.println("线程 " + threadNum + " 尝试放入失败，已存在值: " + existingValue);
                }
            });
        }

        // 关闭线程池并等待任务完成
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.SECONDS);

        System.out.println("\n并发执行完毕后，Map 中 'Job-1' 的最终值:");
        System.out.println(concurrentMap.get(key));
    }
}
