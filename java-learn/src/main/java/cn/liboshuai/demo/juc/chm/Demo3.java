package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 3: ConcurrentHashMap.replace 用法
 * 演示:
 * 1. replace(K key, V value): 仅当 key 存在时，才替换 value (原子操作)
 * 2. replace(K key, V oldValue, V newValue): 仅当 key 存在且 value 等于 oldValue 时，
 * 才替换为 newValue (原子 CAS 操作)
 * (环境: JDK 1.8)
 */
public class Demo3 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        map.put("Flink", "Stateful-Computation");
        map.put("Spark", "In-Memory");

        System.out.println("初始 Map: " + map);

        // --- 1. replace(K key, V value) ---
        System.out.println("\n--- 1. replace(K, V) ---");

        // 尝试替换存在的 key "Flink"
        String prevValue1 = map.replace("Flink", "Stream-Processing");
        System.out.println("替换 'Flink'，返回的旧值: " + prevValue1);
        System.out.println("Map 内容: " + map);

        // 尝试替换不存在的 key "Kafka"
        String prevValue2 = map.replace("Kafka", "Messaging");
        System.out.println("\n替换 'Kafka' (不存在)，返回的旧值: " + prevValue2);
        System.out.println("Map 内容 (未改变): " + map);

        // --- 2. replace(K key, V oldValue, V newValue) ---
        System.out.println("\n--- 2. replace(K, oldV, newV) ---");

        // 尝试替换，但 oldValue 错误
        boolean success1 = map.replace("Spark", "Wrong-Old-Value", "Structured-Streaming");
        System.out.println("尝试替换 'Spark' (oldValue 错误)，是否成功: " + success1);
        System.out.println("Map 内容 (未改变): " + map);

        // 尝试替换，oldValue 正确
        boolean success2 = map.replace("Spark", "In-Memory", "Structured-Streaming");
        System.out.println("\n尝试替换 'Spark' (oldValue 正确)，是否成功: " + success2);
        System.out.println("Map 内容 (已更新): " + map);

        // --- 3. 并发场景演示 (CAS) ---
        System.out.println("\n--- 3. 并发场景 (CAS) ---");
        // 模拟一个计数器
        ConcurrentHashMap<String, Integer> counterMap = new ConcurrentHashMap<>();
        String counterKey = "Job-Success-Count";
        counterMap.put(counterKey, 0);

        ExecutorService pool = Executors.newFixedThreadPool(10);

        // 10个线程并发去增加计数器
        // 这是一个不安全的增加方式，会导致覆盖
        // 我们在下一个 Demo (compute) 中会看到更好的方式
        // 这里用 replace 演示 CAS 如何避免错误更新

        // 先让计数器变为 1
        counterMap.replace(counterKey, 0, 1);
        System.out.println("计数器初始为 1");

        // 假设线程 A 读到 1，线程 B 也读到 1
        // 线程 A 先把 1 更新为 2
        boolean casSuccessA = counterMap.replace(counterKey, 1, 2);
        System.out.println("线程 A 尝试 'replace(key, 1, 2)'，成功: " + casSuccessA);
        System.out.println("Map 内容: " + counterMap);

        // 线程 B 此时也想把 1 更新为 2 (但它拿的是旧值 1)
        boolean casSuccessB = counterMap.replace(counterKey, 1, 2);
        System.out.println("线程 B 尝试 'replace(key, 1, 2)'，成功: " + casSuccessB);
        System.out.println("Map 内容 (未改变): " + counterMap);

        // 线程 B 必须重新 get -> compute -> replace
        // 比如它重新 get 到了 2
        boolean casSuccessB_Retry = counterMap.replace(counterKey, 2, 3);
        System.out.println("线程 B 重试 'replace(key, 2, 3)'，成功: " + casSuccessB_Retry);
        System.out.println("Map 内容 (已更新): " + counterMap);

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.SECONDS);
    }
}
