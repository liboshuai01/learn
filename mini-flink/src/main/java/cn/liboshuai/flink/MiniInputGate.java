package cn.liboshuai.flink;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

public class MiniInputGate {

    private final Queue<NetworkBuffer> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    // 可用性 Future，用于 Mailbox 机制下的唤醒
    private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

    /**
     * [Netty 线程调用] 接受网络层传来的 Buffer
     * 对应 Flink 中的 RemoteInputChannel.onBuffer() -> InputGate.notifyChannelNonEmpty()
     */
    public void onBuffer(NetworkBuffer buffer) {
        lock.lock();
        try {
            queue.add(buffer);
            // 如果有数据且 future 未完成，标记完成以唤醒 Task 线程
            if (!availabilityFuture.isDone()) {
                availabilityFuture.complete(null);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * [Task 线程调用] 尝试获取下一个 Buffer
     */
    public NetworkBuffer pollNext() {
        lock.lock();
        try {
            NetworkBuffer buffer = queue.poll();
            if (queue.isEmpty()) {
                // 队列空了，重置 future，表示“目前不可用”
                if (availabilityFuture.isDone()) {
                    availabilityFuture = new CompletableFuture<>();
                }
            }
            return buffer;
        } finally {
            lock.unlock();
        }
    }

    /**
     * [Task 线程调用] 获取可用性 Future
     */
    public CompletableFuture<Void> getAvailabilityFuture() {
        lock.lock();
        try {
            return availabilityFuture;
        } finally {
            lock.unlock();
        }
    }
}
