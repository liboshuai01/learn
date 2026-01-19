package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Demo 9: ConcurrentHashMap Java 8 批量操作 (Bulk Operations)
 * 演示:
 * 1. forEach(long parallelismThreshold, BiConsumer action): 并行遍历
 * 2. search(long parallelismThreshold, BiFunction searchFunction): 并行搜索
 * 3. reduce(long parallelismThreshold, BiFunction transformer, BiFunction reducer): 并行归约
 * (环境: JDK 1.8)
 */
public class Demo9 {

    // 定义一个很大的并行阈值，强制操作在 "当前线程" 串行执行
    private static final long FORCE_SERIAL_THRESHOLD = Long.MAX_VALUE;

    // 定义一个很小的并行阈值 (1)，强制操作 "尝试并行" 执行 (使用 ForkJoinPool)
    private static final long FORCE_PARALLEL_THRESHOLD = 1L; // 默认值

    public static void main(String[] args) {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        // 填充一些数据
        for (int i = 0; i < 100; i++) {
            map.put("Key-" + i, i);
        }
        map.put("Flink-Special", 999);
        map.put("Spark-Special", -100);

        System.out.println("Map (部分内容): " + map.get("Key-10") + ", " + map.get("Flink-Special"));
        System.out.println("Map size: " + map.size());


        // --- 1. forEach (并行遍历) ---
        System.out.println("\n--- 1. forEach (并行) ---");
        // 使用 FORCE_PARALLEL_THRESHOLD (1)
        // 这个操作会利用 ForkJoinPool 并行地打印键值对
        // 注意：输出的顺序是完全混乱的，因为是并发执行
        System.out.println("开始并行 forEach (顺序会混乱):");
        map.forEach(FORCE_PARALLEL_THRESHOLD, (key, value) -> {
            // (k, v) -> ...
            System.out.println("forEach 线程 " + Thread.currentThread().getName() +
                    ": " + key + " = " + value);
        });
        System.out.println("并行 forEach 完成");


        // --- 2. search (并行搜索) ---
        System.out.println("\n--- 2. search (并行) ---");
        // search 会并行地在 Map 中查找第一个满足条件的元素
        // (k, v) -> U   (U 是返回类型)
        // 一旦找到一个非 null 结果，它会立即返回该结果 (并尝试取消其他正在搜索的线程)

        // 查找第一个 value > 900 的 key
        System.out.println("开始并行 search (查找 value > 900):");
        String foundKey = map.search(FORCE_PARALLEL_THRESHOLD, (key, value) -> {
            if (value > 900) {
                System.out.println("Search 线程 " + Thread.currentThread().getName() + " 找到了: " + key);
                return key; // 返回非 null 结果
            }
            return null; // 继续搜索
        });
        System.out.println("Search 结果: " + foundKey);


        // --- 3. reduce (并行归约) ---
        System.out.println("\n--- 3. reduceValues (并行归约) ---");
        // reduce 是一种更通用的聚合操作
        // 常用的是它的变体: reduceValues, reduceKeys, reduceEntries

        // 示例：并行计算所有 value 的总和
        // 1. transformer: BiFunction (k, v) -> U (转换器)
        //    这里我们只关心 value, 所以 (k, v) -> v
        // 2. reducer: BiFunction (U, U) -> U (合并器)
        //    (v1, v2) -> v1 + v2

        // 这里使用 reduceValuesToInt (更高效的 int 归约)
        long sum = map.reduceValuesToInt(
                FORCE_PARALLEL_THRESHOLD, // 并行
                (value) -> value,         // 1. transformer (这里是 LongToIntFunction)
                0,                        // 2. identity (初始值/默认值)
                (left, right) -> left + right // 3. reducer (IntBinaryOperator)
        );

        System.out.println("并行归约 (reduceValuesToInt) 计算的总和: " + sum);

        // 示例：并行查找最长的 key
        String longestKey = map.reduceKeys(
                FORCE_PARALLEL_THRESHOLD, // 并行
                (key1, key2) -> (key1.length() > key2.length()) ? key1 : key2 // 2. reducer
        );

        System.out.println("并行归约 (reduceKeys) 找到的最长的 Key: " + longestKey);
    }
}
