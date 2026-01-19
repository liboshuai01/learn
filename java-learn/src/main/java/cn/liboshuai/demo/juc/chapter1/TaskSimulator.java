package cn.liboshuai.demo.juc.chapter1;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 实战案例 1：模拟 Flink StreamTask 使用 volatile
 * * 演示 "一个线程写, 一个线程读" 的经典场景。
 * - TaskThread 是 "读" 线程, 它在循环中检查 'running' 标志。
 * - MainThread 是 "写" 线程, 它调用 'stop()' 来修改 'running' 标志。
 * * 思考：
 * 1. 如果把 'running' 的 'volatile' 关键字去掉会发生什么？
 * (提示：JVM 可能会对 'while(running)' 进行优化，导致循环无法停止)
 * 2. 为什么这里不能用 'synchronized'？
 * (提示：性能！如果在 'run()' 循环里加锁，开销巨大)
 */
public class TaskSimulator {

    // 1. 定义一个 Runnable 任务, 就像 Flink 的 StreamTask
    static class StreamTask implements Runnable {

        /**
         * ！！！这就是 Flink 源码中的 'running' 标志！！！
         * 使用 volatile 保证 'running' 变量在多线程间的可见性。
         */
        private volatile boolean running = true;

        private AtomicLong counter = new AtomicLong(0);

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName() + " 开始运行...");

            // 这是 "读" 线程
            // StreamTask 的主循环，模拟不断处理数据
            while (running) {
                // 模拟正在处理数据...
                // 我们在这里加一个小的 sleep 来减缓控制台输出速度
                // 注意：在真实的 Flink 中, 这里是没有 sleep 的,
                // 'running' 的检查会非常频繁。
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                long count = counter.incrementAndGet();
                System.out.println("任务正在运行, Cnt: " + count);
            }

            System.out.println(Thread.currentThread().getName() + " 收到停止信号, 退出循环。");
            System.out.println("总共处理了 " + counter.get() + " 条数据。");
        }

        /**
         * ！！！这是 "写" 线程调用的方法！！！
         * (例如 Flink 的 TaskManager RPC 线程调用 cancel())
         */
        public void stop() {
            System.out.println(Thread.currentThread().getName() + " 正在发送停止信号...");
            this.running = false;
        }

        public long getCounter() {
            return counter.get();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 2. 创建任务实例
        StreamTask myTask = new StreamTask();

        // 3. 创建并启动 "任务线程" (Task Thread)
        Thread taskThread = new Thread(myTask, "TaskThread");
        taskThread.start();

        // 4. 让 "主线程" (Main Thread) 运行 1 秒钟
        // 模拟 Flink Job 运行一段时间
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // 5. "主线程" (Main Thread) 调用 stop() 方法,
        // 就像 JobManager 发送 'cancel' 命令
        myTask.stop();

        // 6. 等待任务线程完全终止
        taskThread.join();

        System.out.println(Thread.currentThread().getName() + " 确认任务已停止。");
    }
}