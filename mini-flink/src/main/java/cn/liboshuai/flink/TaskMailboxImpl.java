package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 邮箱的实现类。
 * 核心修改：在 take 和 tryTake 中增加了对队头元素优先级的检查。
 * 只有当 队头邮件优先级 <= 请求优先级 (priority) 时，才允许取出。
 */
@Slf4j
public class TaskMailboxImpl implements TaskMailbox {

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

    /**
     * 非阻塞获取邮件。
     * 关键逻辑：如果队头邮件的优先级比请求的 priority 低（数值大），则视为"无符合条件的邮件"，返回 Empty。
     */
    @Override
    public Optional<Mail> tryTake(int priority) {
        checkIsMailboxThread();
        lock.lock();
        try {
            Mail head = queue.peek();

            // 1. 物理队列为空
            if (head == null) {
                return Optional.empty();
            }

            // 2. [关键模仿 Flink] 优先级不满足
            // 如果 head.priority (比如 1) > required priority (比如 0)
            // 说明虽然有信，但这封信不够格，不能在这里被取出。
            if (head.getPriority() > priority) {
                return Optional.empty();
            }

            // 3. 满足条件，取出
            return Optional.ofNullable(queue.poll());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 阻塞获取邮件。
     * 关键逻辑：只要队列为空，或者 队头邮件优先级不满足要求，就一直阻塞等待。
     */
    @Override
    public Mail take(int priority) throws InterruptedException {
        checkIsMailboxThread();
        lock.lock();
        try {
            // 循环等待条件：(队列为空) OR (有信，但信的优先级比我要求的低)
            while (isQueueEmptyOrPriorityTooLow(priority)) {
                if (state == State.CLOSED) {
                    throw new IllegalStateException("邮箱已关闭");
                }
                // 阻塞等待 put() 唤醒。
                // 注意：当新邮件放入时，put() 会 signal，此时我们会醒来重新检查 peek()
                notEmpty.await();
            }
            // 走到这里，说明 head != null 且 head.priority <= priority
            return queue.poll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 辅助判断逻辑：是否需要阻塞
     */
    private boolean isQueueEmptyOrPriorityTooLow(int priority) {
        Mail head = queue.peek();
        if (head == null) {
            return true; // 空，需要等
        }
        // 非空，但 head.priority (例如 1-Data) > priority (例如 0-Checkpoint)
        // 说明当前只有低优先级的信，但我想要高优先级的，所以也要等。
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
            // 唤醒可能正在阻塞的 take()
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

    private void checkIsMailboxThread() {
        if (Thread.currentThread() != mailboxThread) {
            throw new IllegalStateException("非法线程访问。预期: " + mailboxThread.getName() + ", 实际: " + Thread.currentThread().getName());
        }
    }
}
