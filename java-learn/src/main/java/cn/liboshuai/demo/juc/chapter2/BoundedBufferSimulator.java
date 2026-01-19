package cn.liboshuai.demo.juc.chapter2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实战案例 3：ReentrantLock + Condition 实现有界缓冲区 (Bounded Buffer)
 * ！！！这是 Flink 网络层 (Network Stack) 的微缩模型！！！
 * 场景映射：
 * - Buffer: 对应 Flink 的 InputChannel (接收数据的通道)。
 * - Producer: 对应 Flink 的 Netty 网络线程 (从网络收到数据, 放入通道)。
 * - Consumer: 对应 Flink 的 StreamTask 线程 (从通道取数据, 进行计算)。
 * 核心知识点：
 * 1. lock.lock() / unlock(): 保证对队列操作的互斥。
 * 2. Condition.await(): 核心！释放锁并挂起当前线程, 直到被 signal。
 * 3. Condition.signal(): 唤醒一个在特定条件上等待的线程。
 * 4. while(condition): 虚假唤醒 (Spurious Wakeup) 的防范, 必须用 while 循环检查条件。
 */
public class BoundedBufferSimulator {
    
    private static final Logger log = LoggerFactory.getLogger(BoundedBufferSimulator.class);
    

    // 模拟 Flink 的 InputChannel
    static class FlinkInputChannel<T> {
        private final Queue<T> buffer = new LinkedList<>();
        private final int capacity;

        // 显式锁
        private final ReentrantLock lock = new ReentrantLock();

        // 条件变量 1: 队列不为空 (给消费者用: 如果空了就等这个条件)
        private final Condition notEmpty = lock.newCondition();

        // 条件变量 2: 队列不为满 (给生产者用: 如果满了就等这个条件)
        // 注意: Flink 的 InputChannel 实际上主要关注 "有数据" (notEmpty),
        // 但在 LocalBufferPool 中申请内存段时, 逻辑类似于等待 "notFull"。
        private final Condition notFull = lock.newCondition();

        public FlinkInputChannel(int capacity) {
            this.capacity = capacity;
        }

        /**
         * 模拟 Netty 线程接收到数据, 写入 Channel
         */
        public void put(T data) throws InterruptedException {
            lock.lock(); // 1. 获取锁
            try {
                // 2. 如果队列满了, 必须等待 "notFull" 条件
                // ！！！必须使用 while 循环, 防止虚假唤醒！！！
                while (buffer.size() == capacity) {
                    log.info(" [生产者]: 缓冲区已满! 等待消费者取走数据...");
                    notFull.await(); // 释放锁, 进入等待状态
                }

                // 3. 放入数据
                buffer.add(data);
                log.info(" [生产者]: 放入数据 " + data + ", 当前大小: " + buffer.size());

                // 4. 唤醒等待数据的消费者 (如果有的话)
                // "嘿, 甚至有一个数据了, 那个因为空而等待的消费者可以醒醒了"
                notEmpty.signal();

            } finally {
                lock.unlock(); // 5. 释放锁
            }
        }

        /**
         * 模拟 StreamTask 线程从 Channel 读取数据
         */
        public T take() throws InterruptedException {
            lock.lock(); // 1. 获取锁
            try {
                // 2. 如果队列空了, 必须等待 "notEmpty" 条件
                // 这直接对应 Flink 源码 InputChannel 中的 while(unprocessedBuffers.isEmpty()) wait();
                while (buffer.isEmpty()) {
                    log.info(" [消费者]: 缓冲区为空! 等待上游数据...");
                    notEmpty.await(); // 释放锁, 进入等待状态
                }

                // 3. 取出数据
                T data = buffer.poll();
                log.info(" [消费者]: 取出数据 " + data + ", 当前大小: " + buffer.size());

                // 4. 唤醒等待空间的生产者 (如果有的话)
                // "嘿, 我取走了一个数据, 腾出了空间, 那个因为满而等待的生产者可以醒醒了"
                notFull.signal();

                return data;
            } finally {
                lock.unlock(); // 5. 释放锁
            }
        }
    }

    public static void main(String[] args) {
        // 创建一个容量只有 5 的缓冲区
        FlinkInputChannel<Integer> channel = new FlinkInputChannel<>(5);

        // 1. 启动 "Netty 生产者" 线程
        Thread nettyThread = new Thread(() -> {
            Random random = new Random();
            try {
                for (int i = 0; i < 20; i++) {
                    channel.put(i);
                    // 模拟网络接收的不确定性延时 (0-100ms)
                    Thread.sleep(random.nextInt(100));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "Netty-Thread");

        // 2. 启动 "Task 消费者" 线程
        Thread taskThread = new Thread(() -> {
            Random random = new Random();
            try {
                for (int i = 0; i < 20; i++) {
                    Integer ignore = channel.take();
                    // 模拟数据处理的耗时 (0-200ms)
                    // 注意: 如果处理比生产慢, 缓冲区很快就会满
                    Thread.sleep(random.nextInt(200));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "StreamTask-Thread");

        nettyThread.start();
        taskThread.start();
    }
}
