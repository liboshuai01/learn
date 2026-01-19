package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 6: ConcurrentHashMap.computeIfPresent 用法
 * 演示:
 * 1. computeIfPresent(K key, BiFunction remappingFunction)
 * 2. 这是一个原子的 "check-then-act" 操作，专门用于更新 (Update)。
 * computeIfPresent 的行为:
 * - 1. 检查 key 是否存在。
 * - 2. 如果 key 不存在:
 * - remappingFunction 完全不会被调用。
 * - 直接返回 null。
 * - 3. 如果 key 存在:
 * - 调用 remappingFunction(key, oldValue) 来计算一个 newValue。
 * - (原子地) 将 (key, newValue) 存入 Map (并返回 newValue)。
 * - (如果 remappingFunction 返回 null，则该 key 会被从 Map 中删除，并返回 null)
 * (环境: JDK 1.8)
 */
public class Demo6 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, Integer> counterMap = new ConcurrentHashMap<>();
        counterMap.put("Flink-Jobs", 5);
        counterMap.put("Spark-Jobs", 10);

        System.out.println("初始 Map: " + counterMap);

        // --- 1. key 存在时 (更新) ---
        System.out.println("\n--- 1. key 存在时 (Flink-Jobs + 1) ---");
        // (k, v) -> v + 1  这里 k 是 "Flink-Jobs", v 是 5
        Integer newFlinkValue = counterMap.computeIfPresent("Flink-Jobs", (key, value) -> {
            System.out.println("computeIfPresent Flink: key=" + key + ", value=" + value);
            return value + 1;
        });
        System.out.println("computeIfPresent 返回的新值: " + newFlinkValue);
        System.out.println("Map 内容: " + counterMap);


        // --- 2. key 不存在时 (无操作) ---
        System.out.println("\n--- 2. key 不存在时 (Kafka-Jobs) ---");
        // "Kafka-Jobs" 不存在，lambda 表达式不会执行
        Integer newKafkaValue = counterMap.computeIfPresent("Kafka-Jobs", (key, value) -> {
            System.out.println("computeIfPresent Kafka: key=" + key + ", value=" + value);
            return value + 1; // 这行代码不会被执行
        });
        System.out.println("computeIfPresent 返回的新值: " + newKafkaValue);
        System.out.println("Map 内容 (未改变): " + counterMap);


        // --- 3. key 存在时，计算结果为 null (删除) ---
        System.out.println("\n--- 3. key 存在时返回 null (删除 Spark-Jobs) ---");
        // (k, v) -> null   这里 k 是 "Spark-Jobs", v 是 10
        Integer newSparkValue = counterMap.computeIfPresent("Spark-Jobs", (key, value) -> {
            System.out.println("computeIfPresent Spark: key=" + key + ", value=" + value);
            return null; // 返回 null 会导致该 key 被移除
        });
        System.out.println("computeIfPresent 返回的新值 (null): " + newSparkValue);
        System.out.println("Map 内容: " + counterMap);


        // --- 4. 并发场景：更新已存在的计数器 ---
        System.out.println("\n--- 4. 并发场景 (更新 Flink-Jobs) ---");
        // 假设 Flink-Jobs 必须已存在 (已初始化)
        ExecutorService pool = Executors.newFixedThreadPool(10);
        int numIncrements = 1000;

        for (int i = 0; i < numIncrements; i++) {
            pool.submit(() -> {
                // 原子地更新一个已存在的 Flink-Jobs 计数器
                counterMap.computeIfPresent("Flink-Jobs", (key, value) -> value + 1);

                // 尝试更新一个不存在的计数器 (不会有任何效果)
                counterMap.computeIfPresent("Hadoop-Jobs", (key, value) -> value + 1);
            });
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n并发执行 " + numIncrements + " 次增加后:");
        System.out.println("Flink-Jobs (初始为 6): " + counterMap.get("Flink-Jobs"));
        System.out.println("Hadoop-Jobs (不存在): " + counterMap.get("Hadoop-Jobs"));
    }
}
