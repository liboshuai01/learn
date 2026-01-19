package cn.liboshuai.demo.juc.chm;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Demo 8: ConcurrentHashMap 安全迭代 (Safe Iteration)
 * 演示:
 * 1. ConcurrentHashMap 的迭代器 (keySet, values, entrySet) 是 "线程安全" 的。
 * 2. 在迭代期间，其他线程可以修改 Map (put, remove)，迭代器不会抛出
 * ConcurrentModificationException。
 * 3. 迭代器是 "弱一致性" (Weakly Consistent) 的：
 * - 迭代器创建后，它保证至少会反映 Map 在迭代器创建时的状态。
 * - 它 "可能" (但不保证) 反映迭代器创建之后发生的修改。
 * - 它保证不会两次返回同一个元素。
 * 4. (更正) 迭代器的 remove() 方法 "是支持的" 并且是 "线程安全的"。
 * (环境: JDK 1.8)
 */
public class Demo8 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        map.put("Flink", 1);
        map.put("Spark", 2);
        map.put("Kafka", 3);
        map.put("Beam", 4);

        System.out.println("初始 Map: " + map);

        // --- 1. 并发修改演示 ---
        System.out.println("\n--- 1. 迭代时并发修改 (演示弱一致性) ---");

        // 创建一个线程，它将在迭代开始后修改 Map
        Thread modifierThread = new Thread(() -> {
            try {
                // 等待 50 毫秒，确保迭代器已经开始
                Thread.sleep(50);
                System.out.println("[Modifier Thread] 正在修改 Map...");

                // 1. 添加一个新元素
                map.put("Hadoop", 5);
                System.out.println("[Modifier Thread] 添加了 'Hadoop'");

                // 2. 删除一个已存在的元素
                map.remove("Spark");
                System.out.println("[Modifier Thread] 移除了 'Spark'");

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // (Main Thread) 开始迭代
        System.out.println("[Main Thread] 开始迭代 entrySet...");

        // 启动修改线程
        modifierThread.start();

        // 使用 for-each 循环 (内部使用迭代器)
        // 这个循环 "不会" 抛出 ConcurrentModificationException
        try {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String key = entry.getKey();
                System.out.println("[Iterator] 遍历到: " + key + " = " + entry.getValue());

                // 在迭代器中 sleep，以确保 Modifier 线程有机会执行
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.err.println("迭代时发生意外错误: " + e);
        }

        System.out.println("[Main Thread] 迭代完成。");

        // 等待修改线程结束
        modifierThread.join();

        System.out.println("\n迭代完成后的 Map 最终状态:");
        System.out.println(map);
        System.out.println("--- (请观察上面 'Hadoop' 和 'Spark' 在迭代中的表现) ---");
        System.out.println("--- (Hadoop 可能出现也可能不出现，Spark 可能被遍历到也可能不被遍历到) ---");


        // --- 2. 迭代器 "支持" remove() ---
        System.out.println("\n--- 2. 迭代器支持 remove() 操作 ---");

        // 假设我们要安全地移除 "Kafka"
        System.out.println("Map (remove 前): " + map);
        Iterator<String> keyIterator = map.keySet().iterator();

        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("Kafka")) {
                System.out.println("正在通过迭代器移除 'Kafka'...");
                // 迭代器的 remove() 是线程安全的
                keyIterator.remove();
            }
        }

        System.out.println("Map (remove 后): " + map);
    }
}