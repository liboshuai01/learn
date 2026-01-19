package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 4: ConcurrentHashMap.compute 用法
 * 演示:
 * 1. compute(K key, BiFunction remappingFunction)
 * 2. remappingFunction: 传入 (key, oldValue)，返回 newValue
 * 3. 这是一个原子的 "read-modify-write" (读-改-写) 操作。
 * remappingFunction 的行为:
 * - (key, oldValue) -> newValue: 更新 key 的值为 newValue
 * - (key, oldValue) -> null:     如果 key 存在，则从 Map 中删除该 key
 * compute 的行为:
 * - 如果 key 存在: remappingFunction 传入 (key, oldValue)，计算结果存入 Map (或删除)
 * - 如果 key 不存在: remappingFunction 传入 (key, null)，计算结果 (null除外) 存入 Map
 * (环境: JDK 1.8)
 */
public class Demo4 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        map.put("Flink", 10);
        map.put("Spark", 20);

        System.out.println("初始 Map: " + map);

        // --- 1. key 存在时，更新值 ---
        // 演示：将 "Flink" 的值 + 1
        System.out.println("\n--- 1. key 存在时更新 (Flink + 1) ---");
        // (k, v) -> v + 1  这里 k 是 "Flink", v 是 10
        Integer newFlinkValue = map.compute("Flink", (key, value) -> {
            System.out.println("compute Flink: key=" + key + ", value=" + value);
            return value + 1;
        });
        System.out.println("compute 返回的新值: " + newFlinkValue);
        System.out.println("Map 内容: " + map);


        // --- 2. key 不存在时，创建值 ---
        // 演示：为 "Kafka" 设
        System.out.println("\n--- 2. key 不存在时创建 (Kafka) ---");
        // (k, v) -> 100   这里 k 是 "Kafka", v 是 null
        Integer newKafkaValue = map.compute("Kafka", (key, value) -> {
            System.out.println("compute Kafka: key=" + key + ", value=" + value);
            // 因为 value 是 null, 我们返回一个新值
            return (value == null) ? 100 : value + 1; // 标准写法
        });
        System.out.println("compute 返回的新值: " + newKafkaValue);
        System.out.println("Map 内容: " + map);


        // --- 3. key 存在时，计算结果为 null (删除) ---
        // 演示：删除 "Spark"
        System.out.println("\n--- 3. key 存在时返回 null (删除 Spark) ---");
        // (k, v) -> null   这里 k 是 "Spark", v 是 20
        Integer newSparkValue = map.compute("Spark", (key, value) -> {
            System.out.println("compute Spark: key=" + key + ", value=" + value);
            return null; // 返回 null 会导致该 key 被移除
        });
        System.out.println("compute 返回的新值 (null): " + newSparkValue);
        System.out.println("Map 内容: " + map);


        // --- 4. key 不存在时，计算结果为 null (无操作) ---
        System.out.println("\n--- 4. key 不存在时返回 null (无操作) ---");
        Integer newHadoopValue = map.compute("Hadoop", (key, value) -> {
            System.out.println("compute Hadoop: key=" + key + ", value=" + value);
            return null; // key 不存在，返回 null，Map 不变
        });
        System.out.println("compute 返回的新值 (null): " + newHadoopValue);
        System.out.println("Map 内容 (未改变): " + map);


        // --- 5. 并发场景：实现原子计数器 ---
        // 这是 compute 最强大的用途之一
        System.out.println("\n--- 5. 并发场景 (原子计数器) ---");
        ConcurrentHashMap<String, Integer> counterMap = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(10);
        String counterKey = "Task-Success-Count";

        int numTasks = 1000;

        for (int i = 0; i < numTasks; i++) {
            pool.submit(() -> {
                // 原子地 "读-改-写"
                // (k, v) -> (v == null) ? 1 : v + 1
                // 如果 v 是 null (第一次)，则设为 1
                // 否则 (v 不是 null)，则设为 v + 1
                counterMap.compute(counterKey, (key, value) -> (value == null) ? 1 : value + 1);
            });
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("执行 " + numTasks + " 次并发增加后，计数器结果:");
        System.out.println(counterMap.get(counterKey));
    }
}
