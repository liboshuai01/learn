package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Demo 1: ConcurrentHashMap 基础操作 (Basic Operations)
 * 演示:
 * 1. 如何创建 ConcurrentHashMap
 * 2. 线程安全的 put 操作
 * 3. 线程安全的 get 操作
 * 4. 线程安全的 remove 操作
 * (环境: JDK 1.8)
 */
public class Demo1 {

    public static void main(String[] args) {
        // 1. 创建一个 ConcurrentHashMap
        // Map<String, Integer> map = new ConcurrentHashMap<>();
        // 为了演示方便，我们直接使用 ConcurrentHashMap 类型
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

        System.out.println("--- 1. put 操作 ---");
        // 2. put 操作
        // put 方法是线程安全的。在 JDK 1.8 中，它通过 CAS 和 synchronized 锁（锁住桶的头节点）
        // 来保证并发写入的原子性，而不是像 1.7 那样锁住整个 Segment。
        map.put("Flink", 1);
        map.put("Spark", 2);
        map.put("Kafka", 3);

        System.out.println("初始 Map 内容: " + map);

        System.out.println("\n--- 2. get 操作 ---");
        // 3. get 操作
        // get 操作是线程安全的，并且通常是非阻塞的（无锁）。
        // 由于 Java 内存模型（JMM）的保证，一个线程的 put 操作对另一个线程的 get 操作是可见的。
        Integer flinkValue = map.get("Flink");
        System.out.println("获取 'Flink' 的值: " + flinkValue);

        Integer nonExistentValue = map.get("Hadoop");
        System.out.println("获取 'Hadoop' (不存在的键) 的值: " + nonExistentValue);


        System.out.println("\n--- 3. remove 操作 ---");
        // 4. remove 操作
        // remove 操作也是线程安全的，使用与 put 类似的锁机制来保证原子性。
        Integer removedValue = map.remove("Spark");
        System.out.println("移除 'Spark'，被移除的值: " + removedValue);

        Integer removedAgain = map.remove("Spark");
        System.out.println("再次移除 'Spark'，被移除的值: " + removedAgain);

        System.out.println("\n--- 最终 Map 内容 ---");
        System.out.println(map);

        // 演示并发安全性 (简单模拟)
        // 实际的并发测试需要使用多线程
        System.out.println("\n--- (演示) put 会覆盖旧值 ---");
        // put 操作会原子地覆盖旧值
        Integer oldValue = map.put("Flink", 118);
        System.out.println("更新 'Flink' 的值，旧值是: " + oldValue);
        System.out.println("更新后的 Map: " + map);
    }
}
