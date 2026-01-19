package cn.liboshuai.demo.juc.threadpool;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class MyBlockingQueue<T> {
    private final Deque<T> queue;
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    public MyBlockingQueue(int capacity) {
        this.capacity = capacity;
        this.queue = new ArrayDeque<>(capacity);
    }

    /**
     * 阻塞添加任务 (一直等待直到放入成功)
     * @param t 任务
     */
    public void put(T t) throws InterruptedException {
        Objects.requireNonNull(t, "任务不能为空");
        lock.lock();
        try {
            while (queue.size() == capacity) {
                notFull.await();
            }
            queue.addLast(t);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞获取任务 (一直等待直到拿到任务)
     * @return 任务
     */
    public T take() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty()) {
                notEmpty.await();
            }
            T t = queue.removeFirst();
            notFull.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞添加任务 (带超时)
     * @param t 任务
     * @param timeout 超时数值
     * @param timeUnit 超时单位
     * @return true: 添加成功; false: 队列满且超时
     */
    public boolean offer(T t, long timeout, TimeUnit timeUnit) throws InterruptedException {
        Objects.requireNonNull(t, "任务不能为空");
        lock.lock();
        try {
            long nanos = timeUnit.toNanos(timeout);
            while (queue.size() == capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            queue.addLast(t);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞获取任务 (带超时)
     * @param timeout 超时数值
     * @param timeUnit 超时单位
     * @return 任务对象, 如果超时则返回null
     */
    public T poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        lock.lock();
        try {
            long nanos = timeUnit.toNanos(timeout);
            while (queue.isEmpty()) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            T t = queue.removeFirst();
            notFull.signal();
            return t;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前队列大小
     */
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }
}
