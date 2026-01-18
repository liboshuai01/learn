package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class TaskMailboxImpl implements TaskMailbox{

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    // 使用 PriorityQueue 保证物理上的顺序：优先级数值小的在队头
    private final PriorityQueue<Mail> queue = new PriorityQueue<>();

    private final Thread mailboxThread;
    private volatile State state = State.OPEN;

    public TaskMailboxImpl(Thread mailboxThread) {
        this.mailboxThread = mailboxThread;
    }

    @Override
    public boolean hasMail() {
        lock.lock();
        try {
            return !queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTake(int priority) {
        checkIsMailboxThread();
        lock.lock();
        try {
            Mail head = queue.peek();
            if (head == null) {
                return Optional.empty();
            }
            if (head.getPriority() > priority) {
                return Optional.empty();
            }
            return Optional.ofNullable(queue.poll());
        } finally {
            lock.unlock();
        }
    }

    private void checkIsMailboxThread() {
        Thread currentThread = Thread.currentThread();
        if (currentThread != mailboxThread) {
            throw new IllegalStateException("非法线程访问。预期: " + mailboxThread.getName() + ", 实际: " + Thread.currentThread().getName());
        }
    }

    @Override
    public Mail take(int priority) throws InterruptedException {
        checkIsMailboxThread();
        lock.lock();
        try {
            while (isQueueEmptyOrPriorityTooLow(priority)) {
                if (state == State.CLOSED) {
                    throw new IllegalStateException("邮件已关闭");
                }
                notEmpty.await();
            }
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }

    private boolean isQueueEmptyOrPriorityTooLow(int priority) {
        Mail head = queue.peek();
        if (head == null) {
            return true;
        }
        return head.getPriority() > priority;
    }

    @Override
    public void put(Mail mail) {
        lock.lock();
        try {
            if (state == State.CLOSED) {
                log.warn("邮箱已关闭，正在丢弃邮件：{}", mail);
                return;
            }
            queue.offer(mail);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            state = State.CLOSED;
            notEmpty.signalAll();
            queue.clear();
        } finally {
            lock.unlock();
        }
    }
}
