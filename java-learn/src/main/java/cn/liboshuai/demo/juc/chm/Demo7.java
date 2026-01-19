package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 7: ConcurrentHashMap.merge 用法
 * 演示:
 * 1. merge(K key, V value, BiFunction remappingFunction)
 * 2. 这是一个原子的 "合并" 或 "聚合" 操作。
 * merge 的行为:
 * - 1. 检查 key 是否存在。
 * - 2. 如果 key 不存在:
 * - (原子地) 将 (key, value) 存入 Map (value 是你传入的第二个参数)。
 * - remappingFunction 完全不会被调用。
 * - 3. 如果 key 存在 (假设值为 oldValue):
 * - 调用 remappingFunction(oldValue, value) 来计算一个 newValue。
 * - (原子地) 将 (key, newValue) 存入 Map。
 * - (如果 remappingFunction 返回 null，则该 key 会被从 Map 中删除)
 * (环境: JDK 1.8)
 */
public class Demo7 {

    public static void main(String[] args) throws InterruptedException {
        // 场景：统计单词频率
        ConcurrentHashMap<String, Integer> wordCounts = new ConcurrentHashMap<>();

        System.out.println("--- 1. key 不存在时 (merge 行为类似 put) ---");
        // key "Flink" 不存在
        // remappingFunction (v1, v2) -> v1 + v2 不会被调用
        // 传入的 value (1) 被直接放入 Map
        wordCounts.merge("Flink", 1, (oldValue, value) -> oldValue + value);
        System.out.println("Map 内容: " + wordCounts);


        System.out.println("\n--- 2. key 存在时 (merge 执行合并) ---");
        // key "Flink" 存在 (oldValue=1)
        // 传入的 value 也是 1
        // remappingFunction 被调用: (1, 1) -> 2
        wordCounts.merge("Flink", 1, (oldValue, value) -> oldValue + value);
        System.out.println("Map 内容: " + wordCounts);

        // 再添加一个 "Spark"
        wordCounts.merge("Spark", 1, (oldValue, value) -> oldValue + value);
        System.out.println("Map 内容: " + wordCounts);

        // 再次 merge "Flink"
        wordCounts.merge("Flink", 1, (oldValue, value) -> oldValue + value);
        System.out.println("Map 内容: " + wordCounts);


        System.out.println("\n--- 3. merge 返回 null (删除) ---");
        // 假设我们用 merge 来 "减少" 计数
        // merge "Spark"，传入 value 为 -1
        // (k, v) -> v + (-1)  这里 k="Spark", oldValue=1, value=-1
        // remappingFunction (1, -1) -> 0
        wordCounts.merge("Spark", -1, (oldValue, value) -> oldValue + value);
        System.out.println("Spark 减 1 后: " + wordCounts);

        // 如果合并函数返回 null，则 key 被删除
        // 假设我们定义：如果计数为 0，则删除
        wordCounts.merge("Spark", -1, (oldValue, value) -> {
            Integer newValue = oldValue + value;
            return (newValue == 0) ? null : newValue; // 如果新值为 0, 返回 null
        });
        System.out.println("Spark 减 1 并返回 null 后 (被删除): " + wordCounts);


        System.out.println("\n--- 4. 并发场景：并发词频统计 ---");
        // 这是 merge 最经典的应用
        ExecutorService pool = Executors.newFixedThreadPool(10);
        ConcurrentHashMap<String, Integer> concurrentCounts = new ConcurrentHashMap<>();
        String[] words = {"Flink", "Spark", "Flink", "Kafka", "Flink", "Spark", "Flink"};

        // 模拟多个线程同时提交单词
        for (String word : words) {
            pool.submit(() -> {
                // 核心：原子地增加计数
                // 传入的 value 固定为 1 (代表 1 次)
                // (oldValue, 1) -> oldValue + 1
                // 如果 key 不存在, oldValue 自动被视为 0 (merge 会自动放入 1)
                concurrentCounts.merge(word, 1, (oldValue, value) -> oldValue + value);

                // 上面这行代码等价于 Demo 4 (compute) 中的:
                // concurrentCounts.compute(word, (k, v) -> (v == null) ? 1 : v + 1);
                // 但 merge 的语义在 "聚合" 时更清晰
            });
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n并发词频统计结果:");
        System.out.println(concurrentCounts);
    }
}
