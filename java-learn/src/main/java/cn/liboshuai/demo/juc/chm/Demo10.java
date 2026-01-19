package cn.liboshuai.demo.juc.chm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo 10: ConcurrentHashMap.size() vs mappingCount()
 * 演示:
 * 1. size(): 返回 Map 中映射的数量，作为 int。
 * 2. mappingCount(): 在 JDK 1.8 引入，返回映射的数量，作为 long。
 * 关键区别:
 * 1. 返回类型: size() 返回 int, mappingCount() 返回 long。
 * - size() 最大返回 Integer.MAX_VALUE (约 21 亿)。如果 Map
 * 真实大小超过这个数，size() 会"撒谎"，返回 Integer.MAX_VALUE。
 * - mappingCount() 返回 long，能正确反映超过 21 亿的元素数量。
 * 2. 精确性 (在并发修改时):
 * - "两者都是估算值"！
 * - 在 JDK 1.8 中，size() 和 mappingCount()
 * 都是通过一种无锁的方式（累加 baseCount 和 CounterCell）来计算大小的。
 * - 如果在计算时有其他线程正在 put 或 remove，它们返回的都可能是一个 "估算值"
 * (不是 100% 精确的瞬时快照)。
 * - 只有当 Map 没有并发修改时，它们的结果才是完全准确的。
 * Flink 源码启示:
 * - 在 Java 8+ 的代码中，应优先使用 mappingCount()，因为它返回 long，语义上更健壮。
 * - 绝不能依赖 size() == 0 来判断 Map 是否为空，应使用 isEmpty()。
 * - 绝不能依赖 size() 或 mappingCount() 的值来进行线程同步或逻辑控制，因为它们在并发下是估算值。
 * (环境: JDK 1.8)
 */
public class Demo10 {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<>();

        // 1. 在 Map 稳定时，两者相等
        System.out.println("--- 1. 稳定 Map ---");
        map.put(1, "A");
        map.put(2, "B");

        int stableSize = map.size();
        long stableMappingCount = map.mappingCount();

        System.out.println("Stable size(): " + stableSize);
        System.out.println("Stable mappingCount(): " + stableMappingCount);
        System.out.println("Stable isEmpty(): " + map.isEmpty());


        // 2. 在 Map 并发修改时，两者都是估算值
        System.out.println("\n--- 2. 并发修改 Map ---");

        // 启动一个线程在后台疯狂添加元素
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(() -> {
            for (int i = 3; i < 10000; i++) {
                map.put(i, "Value-" + i);
            }
        });

        // 主线程在 Map 还在被修改时，尝试读取大小
        System.out.println("开始在并发修改时读取大小 (值会变化):");

        // 我们不能保证读到的是哪个瞬时的值
        for (int i = 0; i < 5; i++) {
            Thread.sleep(1); // 模拟主线程的其他工作
            System.out.println("Snapshot " + i + ": size=" + map.size() +
                    ", mappingCount=" + map.mappingCount());
        }

        // 等待添加线程结束
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n--- 3. 并发修改结束后 ---");
        // 当 Map 再次稳定时，两者结果再次准确且相等
        System.out.println("Final size(): " + map.size());
        System.out.println("Final mappingCount(): " + map.mappingCount());

        // 3. size() vs isEmpty()
        // 为什么不用 size() == 0?
        // 因为 size() 在并发下是估算值，它可能短暂地返回 0 (即使 Map 中有元素)
        // 或 >0 (即使 Map 刚被清空)。
        // isEmpty() 通常更快，且能提供更准确的 "是否为空" 的即时判断。
        System.out.println("\n--- 4. 清空 Map ---");
        map.clear();
        System.out.println("After clear(), size(): " + map.size());
        System.out.println("After clear(), mappingCount(): " + map.mappingCount());
        System.out.println("After clear(), isEmpty(): " + map.isEmpty());
    }
}
