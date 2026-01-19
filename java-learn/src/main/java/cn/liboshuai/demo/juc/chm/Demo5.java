package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo 5: ConcurrentHashMap.computeIfAbsent 用法
 * 演示:
 * 1. computeIfAbsent(K key, Function mappingFunction)
 * 2. 这是一个原子的 "check-then-act" (检查后执行) 操作，专门用于创建 (Create)。
 * computeIfAbsent 的行为:
 * - 1. 检查 key 是否存在。
 * - 2. 如果 key 存在:
 * - mappingFunction 完全不会被调用。
 * - 直接返回 Map 中已有的 value。
 * - 3. 如果 key 不存在:
 * - 调用 mappingFunction(key) 来计算一个 newValue。
 * - (原子地) 将 (key, newValue) 存入 Map。
 * - 返回这个 newValue。
 * - (如果 mappingFunction 返回 null，则 Map 不变，并返回 null)
 * (环境: JDK 1.8)
 */
public class Demo5 {

    // 模拟一个昂贵的创建操作
    private static String createExpensiveResource(String key) {
        System.out.println("!!! 正在调用昂贵的创建操作来创建: " + key + " !!!");
        // 模拟耗时
        try {
            Thread.sleep(500); // 500ms
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "Resource-For-" + key;
    }

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, String> resourceCache = new ConcurrentHashMap<>();

        // --- 1. 基本用法 (key 不存在) ---
        System.out.println("--- 1. key 不存在时 (Flink) ---");
        // k -> createExpensiveResource(k)  这里 k 是 "Flink"
        // 因为 "Flink" 不存在，mappingFunction 会被调用
        String flinkResource = resourceCache.computeIfAbsent("Flink",
                key -> createExpensiveResource(key));

        System.out.println("获取到的 Flink 资源: " + flinkResource);
        System.out.println("缓存 Map 内容: " + resourceCache);


        // --- 2. 基本用法 (key 已存在) ---
        System.out.println("\n--- 2. key 已存在时 (Flink) ---");
        // 再次获取 "Flink"
        // 因为 "Flink" 已经存在，mappingFunction (createExpensiveResource) 不会被调用
        String flinkResourceCached = resourceCache.computeIfAbsent("Flink",
                key -> createExpensiveResource(key)); // 这行代码不会被执行

        System.out.println("再次获取 Flink 资源: " + flinkResourceCached);
        System.out.println("缓存 Map 内容 (未改变): " + resourceCache);


        // --- 3. 并发场景：懒加载缓存 ---
        System.out.println("\n--- 3. 并发场景 (懒加载 Spark) ---");
        // 10个线程同时尝试获取 "Spark" 资源
        // 只有第一个到达的线程会执行 createExpensiveResource
        // 其他线程会等待它执行完毕，然后获取它创建的值

        ExecutorService pool = Executors.newFixedThreadPool(10);
        String resourceKey = "Spark";

        // 用于计创建操作被调用了多少次
        AtomicInteger creationCount = new AtomicInteger(0);

        for (int i = 0; i < 10; i++) {
            final int threadNum = i;
            pool.submit(() -> {
                System.out.println("线程 " + threadNum + " 尝试获取 " + resourceKey);

                String resource = resourceCache.computeIfAbsent(resourceKey, key -> {
                    // 这部分代码 (mappingFunction) 是在锁内执行的
                    // 它只会被调用一次
                    creationCount.incrementAndGet();
                    return createExpensiveResource(key); // 调用昂贵操作
                });

                System.out.println("线程 " + threadNum + " 成功获取到资源: " + resource);
            });
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n并发执行完毕后:");
        System.out.println("昂贵的创建操作总共被调用了 " + creationCount.get() + " 次。");
        System.out.println("缓存 Map 最终内容: " + resourceCache);
    }
}
