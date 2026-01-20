package cn.liboshuai.learn.flink.mini;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MiniFlink {

    @FunctionalInterface
    interface ThrowingRunnable<E extends Exception> {
        void run() throws E;
    }

    /**
     * 封装了具体的任务（Runnable）和优先级。
     * 修改点：实现了 Comparable 接口，并增加了 seqNum 以保证同优先级的 FIFO 顺序。
     */
    static class Mail implements Comparable<Mail> {

        // 全局递增序列号，用于保证相同优先级邮件的提交顺序 (FIFO)
        private static final AtomicLong SEQ_COUNTER = new AtomicLong();

        // 真正的业务逻辑 (例如：执行 Checkpoint，或者处理一条数据)
        // 注意：这里假设 ThrowingRunnable 定义在包级别或 MiniFlink 中
        private final ThrowingRunnable<? extends Exception> runnable;

        // 优先级 (数字越小优先级越高)
        @Getter
        private final int priority;

        // 描述信息，用于调试 (例如 "Checkpoint 15")
        private final String description;

        // 序列号：在创建时生成，用于解决 PriorityQueue 不稳定排序的问题
        private final long seqNum;

        public Mail(ThrowingRunnable<? extends Exception> runnable, int priority, String description) {
            this.runnable = runnable;
            this.priority = priority;
            this.description = description;
            // 获取当前唯一递增序号
            this.seqNum = SEQ_COUNTER.getAndIncrement();
        }

        /**
         * 执行邮件中的逻辑
         */
        public void run() throws Exception {
            runnable.run();
        }

        @Override
        public String toString() {
            return description + " (priority=" + priority + ", seq=" + seqNum + ")";
        }

        /**
         * 优先级比较核心逻辑 (仿照 Flink 1.18)
         * 1. 优先比较 priority (数值越小，优先级越高)
         * 2. 如果 priority 相同，比较 seqNum (数值越小，提交越早，越先执行)
         */
        @Override
        public int compareTo(Mail other) {
            int priorityCompare = Integer.compare(this.priority, other.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // 优先级相同，严格按照 FIFO
            return Long.compare(this.seqNum, other.seqNum);
        }
    }

    interface TaskMailbox {

        /**
         * 邮箱是否包含邮件
         */
        boolean hasMail();

        /**
         * 非阻塞式获取邮件 (如果没有则返回 Empty)
         */
        Optional<Mail> tryTake(int priority);

        /**
         * 阻塞式获取邮件 (如果为空则等待，直到有邮件或邮箱关闭)
         * 必须由主线程调用
         */
        Mail take(int priority) throws InterruptedException;

        /**
         * 放入邮件 (任何线程都可调用)
         */
        void put(Mail mail);

        /**
         * 关闭邮箱，不再接受新邮件，并唤醒所有等待线程
         */
        void close();

        /**
         * 邮箱状态
         */
        enum State {
            OPEN, QUIESCED, // 暂停处理
            CLOSED    // 彻底关闭
        }
    }

    /**
     * 邮箱的实现类。
     * 核心修改：在 take 和 tryTake 中增加了对队头元素优先级的检查。
     * 只有当 队头邮件优先级 <= 请求优先级 (priority) 时，才允许取出。
     */
    @Slf4j
    static class TaskMailboxImpl implements TaskMailbox {

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

    interface MailboxDefaultAction {

        /**
         * 执行默认动作。
         *
         * @param controller 用于与主循环交互（例如请求挂起）
         */
        void runDefaultAction(Controller controller) throws Exception;

        /**
         * 控制器：允许 DefaultAction 影响主循环的行为
         */
        interface Controller {
            /**
             * 告诉主循环：“我没活干了（Input 为空），请暂停调度我。”
             * 调用此方法后，主循环将不再调用 runDefaultAction，直到被 resume。
             */
            void suspendDefaultAction();
        }
    }

    interface MailboxExecutor {

        /**
         * 提交一个任务到邮箱。
         *
         * @param command     业务逻辑
         * @param description 调试描述
         */
        void execute(ThrowingRunnable<? extends Exception> command, String description);
    }

    static class MailboxExecutorImpl implements MailboxExecutor {

        private final TaskMailbox mailbox;
        private final int priority;

        public MailboxExecutorImpl(TaskMailbox mailbox, int priority) {
            this.mailbox = mailbox;
            this.priority = priority;
        }

        @Override
        public void execute(ThrowingRunnable<? extends Exception> command, String description) {
            // 包装成 Mail 并扔进邮箱
            mailbox.put(new Mail(command, priority, description));
        }

    }

    /**
     * 邮箱处理器。
     * 贴近 Flink 1.18：Controller 不再负责让步逻辑，只负责挂起。
     * 让步逻辑交由 Action 自己通过 checkMailbox 实现。
     */
    static class MailboxProcessor implements MailboxDefaultAction.Controller {

        public static final int MIN_PRIORITY = 0;
        public static final int DEFAULT_PRIORITY = 1;

        private final MailboxDefaultAction defaultAction;
        private final TaskMailbox mailbox;

        @Getter
        private final MailboxExecutor mainExecutor;

        private boolean isDefaultActionAvailable = true;

        public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
            this.defaultAction = defaultAction;
            this.mailbox = mailbox;
            this.mainExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY);
        }

        public void runMailboxLoop() throws Exception {
            while (true) {
                // 阶段 1: 处理所有待处理的邮件 (Checkpoint, Timers 等)
                // 只要有邮件，就一直处理，直到邮箱为空或只剩下优先级不够的邮件
                while (processMail(mailbox, MIN_PRIORITY)) {
                    // loop
                }

                // 阶段 2: 执行默认动作 (数据处理)
                if (isDefaultActionAvailable) {
                    // StreamInputProcessor 在内部进行批处理时，会主动检查 mailbox.hasMail()
                    defaultAction.runDefaultAction(this);
                } else {
                    // 阶段 3: 挂起，阻塞等待新邮件
                    Mail mail = mailbox.take(DEFAULT_PRIORITY);
                    mail.run();
                }
            }
        }

        private boolean processMail(TaskMailbox mailbox, int priority) throws Exception {
            Optional<Mail> mail = mailbox.tryTake(priority);
            if (mail.isPresent()) {
                mail.get().run();
                return true;
            }
            return false;
        }

        // --- Controller 接口实现 ---

        @Override
        public void suspendDefaultAction() {
            this.isDefaultActionAvailable = false;
        }

        public void resumeDefaultAction() {
            mailbox.put(new Mail(() -> this.isDefaultActionAvailable = true, MIN_PRIORITY, "Resume Default Action"));
        }
    }

    /**
     * 对应 Flink 的 ProcessingTimeCallback。
     * 当定时器触发时调用的回调接口。
     */
    @FunctionalInterface
    public interface ProcessingTimeCallback {
        /**
         * 当处理时间到达预定时间戳时调用。
         *
         * @param timestamp 触发的时间戳
         */
        void onProcessingTime(long timestamp) throws Exception;
    }

    /**
     * 对应 Flink 的 ProcessingTimeService。
     * 提供对处理时间（Processing Time）的访问和定时器注册服务。
     */
    public interface ProcessingTimeService {

        /**
         * 获取当前的处理时间（通常是系统时间）。
         */
        long getCurrentProcessingTime();

        /**
         * 注册一个定时器，在指定的时间戳触发回调。
         *
         * @param timestamp 触发时间
         * @param callback  回调逻辑
         * @return 用于取消定时器的 Future
         */
        ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback);

        /**
         * 关闭服务，释放资源（如线程池）。
         */
        void shutdownService();
    }

    /**
     * 对应 Flink 的 SystemProcessingTimeService。
     * 使用 ScheduledThreadPoolExecutor 及其后台线程来触发定时任务。
     */
    @Slf4j
    static class SystemProcessingTimeService implements ProcessingTimeService {

        private final ScheduledExecutorService timerService;

        public SystemProcessingTimeService() {
            // 创建一个核心线程数为 1 的调度线程池，类似 Flink 的默认行为
            this.timerService = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "Flink-System-Timer-Service");
                t.setDaemon(true);
                return t;
            });
        }

        @Override
        public long getCurrentProcessingTime() {
            return System.currentTimeMillis();
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            long delay = Math.max(0, timestamp - getCurrentProcessingTime());

            // 提交到调度线程池
            return timerService.schedule(() -> {
                try {
                    callback.onProcessingTime(timestamp);
                } catch (Exception e) {
                    log.error("定时器回调执行异常", e);
                }
            }, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public void shutdownService() {
            if (!timerService.isShutdown()) {
                timerService.shutdownNow();
            }
        }
    }

    /**
     * 任务基类。
     * 修改点：
     * 1. 增加了 ProcessingTimeService 的初始化和关闭。
     * 2. 提供了 getProcessingTimeService() 供子类使用。
     */
    @Slf4j
    static abstract class StreamTask implements MailboxDefaultAction {

        protected final TaskMailbox mailbox;
        protected final MailboxProcessor mailboxProcessor;
        protected final MailboxExecutor mainMailboxExecutor;

        // [新增] 定时器服务
        protected final ProcessingTimeService timerService;

        public StreamTask() {
            Thread currentThread = Thread.currentThread();
            this.mailbox = new TaskMailboxImpl(currentThread);
            this.mailboxProcessor = new MailboxProcessor(this, mailbox);
            this.mainMailboxExecutor = mailboxProcessor.getMainExecutor();

            // [新增] 初始化定时器服务
            this.timerService = new SystemProcessingTimeService();
        }

        public final void invoke() throws Exception {
            log.info("[StreamTask] 任务已启动。");
            try {
                mailboxProcessor.runMailboxLoop();
            } catch (Exception e) {
                log.error("[StreamTask] 异常：{}", e.getMessage());
                throw e;
            } finally {
                close();
            }
        }

        private void close() {
            log.info("[StreamTask] 结束。");
            // [新增] 关闭定时器服务资源
            if (timerService != null) {
                timerService.shutdownService();
            }
            mailbox.close();
        }

        public MailboxExecutor getControlMailboxExecutor() {
            return new MailboxExecutorImpl(mailbox, MailboxProcessor.MIN_PRIORITY);
        }

        // [新增] 暴露给子类使用
        public ProcessingTimeService getProcessingTimeService() {
            return timerService;
        }

        @Override
        public abstract void runDefaultAction(Controller controller) throws Exception;

        public abstract void performCheckpoint(long checkpointId);
    }

    /**
     * 模拟 Flink 的网络 Buffer (org.apache.flink.runtime.io.network.buffer.Buffer)。
     * 在真实 Flink 中，这里封装的是 MemorySegment (堆外内存/堆内存)。
     * 这里为了简化，我们内部持有一个字节数组来模拟数据。
     */
    public static class NetworkBuffer {

        private final byte[] data;

        public NetworkBuffer(String content) {
            this.data = content.getBytes(StandardCharsets.UTF_8);
        }

        public NetworkBuffer(byte[] data) {
            this.data = data;
        }

        public int getSize() {
            return data.length;
        }

        public byte[] getBytes() {
            return data;
        }

        public String toStringContent() {
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NetworkBufferAllocator
     * 用于在 Netty 解码阶段分配 NetworkBuffer，承载接收到的数据。
     */
    public interface NetworkBufferAllocator {

        /**
         * 分配一个用于接收数据的 Buffer。
         * 在 Flink 中这里会请求 MemorySegment，这里我们简化为创建一个包装了 ByteBuf 的 MiniFlink.NetworkBuffer。
         */
        MiniFlink.NetworkBuffer allocatePooledNetworkBuffer();

        /**
         * 简单的默认实现
         */
        class SimpleAllocator implements NetworkBufferAllocator {
            @Override
            public MiniFlink.NetworkBuffer allocatePooledNetworkBuffer() {
                // 在 MiniFlink 中，NetworkBuffer 内部持有 byte[]
                // 为了配合 Netty 的 Zero-copy 写入，这里的实现稍作变通，
                // 实际解码器会填充数据到这个 Buffer 中。
                return new MiniFlink.NetworkBuffer(new byte[0]);
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessage
     * 定义网络传输协议。
     * * 协议格式:
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     */
    public static abstract class NettyMessage {

        // Flink 经典魔数
        public static final int MAGIC_NUMBER = 0xBADC0FFE;
        // Frame 头部长度: Length(4) + Magic(4) + ID(1)
        public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;

        // --- 消息 ID 定义 ---
        public static final byte ID_BUFFER_RESPONSE = 0;
        public static final byte ID_ERROR_RESPONSE = 1;
        public static final byte ID_PARTITION_REQUEST = 2;
        public static final byte ID_ADD_CREDIT = 6; // 对应 Flink 的 AddCredit

        abstract void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) throws IOException;

        // ------------------------------------------------------------------------
        //  具体消息类型
        // ------------------------------------------------------------------------

        /**
         * 服务端 -> 客户端：发送数据
         */
        public static class BufferResponse extends NettyMessage {
            public final MiniFlink.NetworkBuffer buffer;
            public final int sequenceNumber;
            public final int backlog; // 积压量，用于 Credit 机制

            public BufferResponse(MiniFlink.NetworkBuffer buffer, int sequenceNumber, int backlog) {
                this.buffer = buffer;
                this.sequenceNumber = sequenceNumber;
                this.backlog = backlog;
            }

            boolean isBuffer() {
                return true;
            }

            @Override
            void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
                byte[] data = buffer.getBytes();
                int dataLen = data.length;

                // 1. 分配 Header Buffer
                // Header: [Sequence(4)] + [Backlog(4)] + [Size(4)] = 12 bytes
                int headerLen = 4 + 4 + 4;
                ByteBuf headerBuf = allocator.directBuffer(FRAME_HEADER_LENGTH + headerLen);

                // 2. 写 Frame Header
                headerBuf.writeInt(FRAME_HEADER_LENGTH + headerLen + dataLen - 4); // Frame Length (不包含自身4字节)
                headerBuf.writeInt(MAGIC_NUMBER);
                headerBuf.writeByte(ID_BUFFER_RESPONSE);

                // 3. 写 Message Header
                headerBuf.writeInt(sequenceNumber);
                headerBuf.writeInt(backlog);
                headerBuf.writeInt(dataLen);

                // 4. 发送 Header
                ctx.write(headerBuf);

                // 5. 发送 Body (Zero-Copy: 直接 wrap 字节数组)
                ByteBuf dataBuf = io.netty.buffer.Unpooled.wrappedBuffer(data);
                ctx.write(dataBuf, promise);
            }

            // 静态读取方法 (仅读取 Message Header 部分，Body 由 Decoder 处理)
            public static BufferResponse readFrom(ByteBuf buffer) {
                int seq = buffer.readInt();
                int backlog = buffer.readInt();
                int size = buffer.readInt();
                // 注意：这里返回的 BufferResponse 暂时还没有数据，数据由 Decoder 填充
                // 我们用一个空的 NetworkBuffer 占位，或者用专门的 Builder
                return new BufferResponse(new MiniFlink.NetworkBuffer(new byte[size]), seq, backlog);
            }
        }

        /**
         * 客户端 -> 服务端：请求分区
         */
        public static class PartitionRequest extends NettyMessage {
            public final int partitionId;
            public final int credit;

            public PartitionRequest(int partitionId, int credit) {
                this.partitionId = partitionId;
                this.credit = credit;
            }

            @Override
            void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
                ByteBuf buf = allocator.directBuffer();
                // PartitionId(4) + Credit(4)
                int msgLen = 4 + 4;

                buf.writeInt(FRAME_HEADER_LENGTH + msgLen - 4);
                buf.writeInt(MAGIC_NUMBER);
                buf.writeByte(ID_PARTITION_REQUEST);

                buf.writeInt(partitionId);
                buf.writeInt(credit);

                ctx.write(buf, promise);
            }

            public static PartitionRequest readFrom(ByteBuf buffer) {
                int pid = buffer.readInt();
                int credit = buffer.readInt();
                return new PartitionRequest(pid, credit);
            }
        }

        /**
         * 服务端 -> 客户端：错误响应
         */
        public static class ErrorResponse extends NettyMessage {
            public final String message;

            public ErrorResponse(Throwable cause) {
                this.message = cause.getMessage() == null ? cause.toString() : cause.getMessage();
            }

            public ErrorResponse(String message) {
                this.message = message;
            }

            @Override
            void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
                byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
                ByteBuf buf = allocator.directBuffer();

                buf.writeInt(FRAME_HEADER_LENGTH + 4 + bytes.length - 4);
                buf.writeInt(MAGIC_NUMBER);
                buf.writeByte(ID_ERROR_RESPONSE);

                buf.writeInt(bytes.length);
                buf.writeBytes(bytes);

                ctx.write(buf, promise);
            }

            public static ErrorResponse readFrom(ByteBuf buffer) {
                int len = buffer.readInt();
                byte[] bytes = new byte[len];
                buffer.readBytes(bytes);
                return new ErrorResponse(new String(bytes, StandardCharsets.UTF_8));
            }
        }

        /**
         * 客户端 -> 服务端：增加信用 (AddCredit)
         */
        public static class AddCredit extends NettyMessage {
            public final int credit;

            public AddCredit(int credit) {
                this.credit = credit;
            }

            @Override
            void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise, ByteBufAllocator allocator) {
                ByteBuf buf = allocator.directBuffer();
                int msgLen = 4; // Credit(4)

                buf.writeInt(FRAME_HEADER_LENGTH + msgLen - 4);
                buf.writeInt(MAGIC_NUMBER);
                buf.writeByte(ID_ADD_CREDIT);
                buf.writeInt(credit);

                ctx.write(buf, promise);
            }

            public static AddCredit readFrom(ByteBuf buffer) {
                return new AddCredit(buffer.readInt());
            }
        }

        // ------------------------------------------------------------------------
        //  Encoder (所有消息通用的编码器)
        // ------------------------------------------------------------------------

        @ChannelHandler.Sharable
        public static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                if (msg instanceof NettyMessage) {
                    ((NettyMessage) msg).write(ctx, msg, promise, ctx.alloc());
                } else {
                    ctx.write(msg, promise);
                }
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessageDecoder
     * 特定消息解码器的基类。
     */
    public static abstract class NettyMessageDecoder {

        /** 正在解码的消息 ID */
        protected int msgId;

        /** 正在解码的消息总长度 (包含 Header 和 Body) */
        protected int messageLength;

        /**
         * 通知有新消息到达。
         * @param msgId 消息 ID
         * @param messageLength 消息长度
         */
        public void onNewMessageReceived(int msgId, int messageLength) {
            this.msgId = msgId;
            this.messageLength = messageLength;
        }

        /**
         * 处理读取到的数据片段。
         * @param data 接收到的 ByteBuf
         * @return 解码结果
         */
        public abstract DecodingResult onChannelRead(ByteBuf data) throws Exception;

        public abstract void close();

        /**
         * 解码结果封装
         */
        public static class DecodingResult {
            private final boolean finished;
            private final NettyMessage message;

            private DecodingResult(boolean finished, NettyMessage message) {
                this.finished = finished;
                this.message = message;
            }

            public static DecodingResult notFinished() {
                return new DecodingResult(false, null);
            }

            public static DecodingResult fullMessage(NettyMessage message) {
                return new DecodingResult(true, message);
            }

            public boolean isFinished() {
                return finished;
            }

            public NettyMessage getMessage() {
                return message;
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.BufferResponseDecoder
     * 专门用于解码 BufferResponse。
     * 特点：分为两个阶段，先解消息头(Header)，再解数据体(Body)。允许数据跨多个 TCP 包到达。
     */
    public static class BufferResponseDecoder extends NettyMessageDecoder {

        private final NetworkBufferAllocator allocator;

        // 缓存 Message Header 的 Buffer (Sequence + Backlog + Size)
        private ByteBuf headerBuffer;

        // 当前正在构建的响应对象
        private NettyMessage.BufferResponse currentResponse;

        // 已经读取的数据体长度
        private int accumulatedBodyBytes;

        public BufferResponseDecoder(NetworkBufferAllocator allocator) {
            this.allocator = allocator;
        }

        public void onChannelActive(ChannelHandlerContext ctx) {
            // 12 字节 = Sequence(4) + Backlog(4) + Size(4)
            if (headerBuffer == null) {
                headerBuffer = ctx.alloc().directBuffer(12);
            }
        }

        @Override
        public DecodingResult onChannelRead(ByteBuf data) {
            // 阶段 1: 解码消息头 (Message Header)
            if (currentResponse == null) {
                // 尝试读取足够的字节到 headerBuffer
                int needed = 12 - headerBuffer.readableBytes();
                int toCopy = Math.min(needed, data.readableBytes());
                data.readBytes(headerBuffer, toCopy);

                if (headerBuffer.readableBytes() < 12) {
                    return DecodingResult.notFinished();
                }

                // Header 读满了，解析它
                currentResponse = NettyMessage.BufferResponse.readFrom(headerBuffer);
                headerBuffer.clear();
                accumulatedBodyBytes = 0;
            }

            // 阶段 2: 解码数据体 (Body)
            if (currentResponse != null) {
                int bodySize = currentResponse.buffer.getSize(); // 这里的 Size 是从 Header 读出来的
                int remaining = bodySize - accumulatedBodyBytes;

                int toRead = Math.min(remaining, data.readableBytes());
                if (toRead > 0) {
                    // 将数据读入 NetworkBuffer 的底层数组中
                    // 注意：在真实 Flink 中是 composite buffer 或直接写入 native memory
                    // 这里为了 Mini 模拟，我们直接写入 byte[]
                    byte[] target = currentResponse.buffer.getBytes();
                    data.readBytes(target, accumulatedBodyBytes, toRead);
                    accumulatedBodyBytes += toRead;
                }

                if (accumulatedBodyBytes >= bodySize) {
                    NettyMessage.BufferResponse fullMsg = currentResponse;
                    currentResponse = null; // 重置状态，准备读下一条
                    return DecodingResult.fullMessage(fullMsg);
                }
            }

            return DecodingResult.notFinished();
        }

        @Override
        public void close() {
            if (headerBuffer != null) {
                headerBuffer.release();
                headerBuffer = null;
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NonBufferResponseDecoder
     * 处理简单的控制消息（PartitionRequest, AddCredit, Error 等）。
     * 这些消息通常很短，我们直接读取全部字节后解析。
     */
    public static class NonBufferResponseDecoder extends NettyMessageDecoder {

        private ByteBuf accumulationBuf;

        public void onChannelActive(ChannelHandlerContext ctx) {
            if (accumulationBuf == null) {
                accumulationBuf = ctx.alloc().directBuffer(128); // 初始给个小空间
            }
        }

        @Override
        public void onNewMessageReceived(int msgId, int messageLength) {
            super.onNewMessageReceived(msgId, messageLength);
            accumulationBuf.clear();
            // 确保容量足够
            if (accumulationBuf.capacity() < messageLength) {
                accumulationBuf.capacity(messageLength);
            }
        }

        @Override
        public DecodingResult onChannelRead(ByteBuf data) {
            // 将收到的数据累积起来
            int needed = messageLength - accumulationBuf.readableBytes();
            int toCopy = Math.min(needed, data.readableBytes());
            data.readBytes(accumulationBuf, toCopy);

            if (accumulationBuf.readableBytes() < messageLength) {
                return DecodingResult.notFinished();
            }

            // 数据读齐了，开始根据 msgId 解析
            NettyMessage msg;
            switch (msgId) {
                case NettyMessage.ID_PARTITION_REQUEST:
                    msg = NettyMessage.PartitionRequest.readFrom(accumulationBuf);
                    break;
                case NettyMessage.ID_ERROR_RESPONSE:
                    msg = NettyMessage.ErrorResponse.readFrom(accumulationBuf);
                    break;
                case NettyMessage.ID_ADD_CREDIT:
                    msg = NettyMessage.AddCredit.readFrom(accumulationBuf);
                    break;
                default:
                    throw new IllegalStateException("未知的消息 ID: " + msgId);
            }

            return DecodingResult.fullMessage(msg);
        }

        @Override
        public void close() {
            if (accumulationBuf != null) {
                accumulationBuf.release();
                accumulationBuf = null;
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyMessageClientDecoderDelegate
     * * 这是一个 Netty Inbound Handler，负责：
     * 1. 解码 Frame Header (Length + Magic + ID)。
     * 2. 根据 ID 将剩余数据的解码工作委托给 `BufferResponseDecoder` 或 `NonBufferResponseDecoder`。
     */
    @Slf4j
    public static class NettyMessageClientDecoderDelegate extends ChannelInboundHandlerAdapter {

        private final BufferResponseDecoder bufferResponseDecoder;
        private final NonBufferResponseDecoder nonBufferResponseDecoder;

        // Frame Header 缓存
        private ByteBuf frameHeaderBuffer;

        // 当前正在使用的解码器
        private NettyMessageDecoder currentDecoder;

        public NettyMessageClientDecoderDelegate(NetworkBufferAllocator allocator) {
            this.bufferResponseDecoder = new BufferResponseDecoder(allocator);
            this.nonBufferResponseDecoder = new NonBufferResponseDecoder();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            frameHeaderBuffer = ctx.alloc().directBuffer(NettyMessage.FRAME_HEADER_LENGTH);
            bufferResponseDecoder.onChannelActive(ctx);
            nonBufferResponseDecoder.onChannelActive(ctx);
            super.channelActive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (!(msg instanceof ByteBuf)) {
                ctx.fireChannelRead(msg);
                return;
            }

            ByteBuf data = (ByteBuf) msg;
            try {
                while (data.isReadable()) {
                    // 1. 如果有选定的解码器，先让它跑
                    if (currentDecoder != null) {
                        NettyMessageDecoder.DecodingResult result = currentDecoder.onChannelRead(data);
                        if (result.isFinished()) {
                            // 解码完成，将对象向下传递
                            ctx.fireChannelRead(result.getMessage());
                            currentDecoder = null;
                            frameHeaderBuffer.clear();
                        } else {
                            // 数据不够，等下一次 channelRead
                            break;
                        }
                    }

                    // 2. 如果没有在解码 Body，说明需要解 Frame Header
                    if (currentDecoder == null) {
                        int needed = NettyMessage.FRAME_HEADER_LENGTH - frameHeaderBuffer.readableBytes();
                        int toCopy = Math.min(needed, data.readableBytes());
                        data.readBytes(frameHeaderBuffer, toCopy);

                        if (frameHeaderBuffer.readableBytes() == NettyMessage.FRAME_HEADER_LENGTH) {
                            // Header 读齐了，解析它
                            int frameLength = frameHeaderBuffer.readInt(); // 总帧长
                            int magic = frameHeaderBuffer.readInt();
                            byte msgId = frameHeaderBuffer.readByte();

                            if (magic != NettyMessage.MAGIC_NUMBER) {
                                throw new IllegalStateException("魔数错误，连接可能已损坏");
                            }

                            // 消息体长度 = 总帧长 - (Magic 4 + ID 1)
                            int messageBodyLength = frameLength - 5;

                            // 选择解码器
                            if (msgId == NettyMessage.ID_BUFFER_RESPONSE) {
                                currentDecoder = bufferResponseDecoder;
                            } else {
                                currentDecoder = nonBufferResponseDecoder;
                            }

                            // 初始化解码器状态
                            currentDecoder.onNewMessageReceived(msgId, messageBodyLength);
                        } else {
                            // Header 没读齐，跳出等待更多数据
                            break;
                        }
                    }
                }
            } finally {
                data.release();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (frameHeaderBuffer != null) frameHeaderBuffer.release();
            bufferResponseDecoder.close();
            nonBufferResponseDecoder.close();
            super.channelInactive(ctx);
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler
     * * 职责：
     * 1. 接收 BufferResponse，并将其放入 InputGate。
     * 2. 接收 ErrorResponse。
     * 3. 管理 Credit (简化版：这里我们简单地在连接建立时发送一次 Request)。
     */
    @Slf4j
    public static class CreditBasedPartitionRequestClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

        private final MiniFlink.MiniInputGate inputGate;

        public CreditBasedPartitionRequestClientHandler(MiniFlink.MiniInputGate inputGate) {
            this.inputGate = inputGate;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("[Client] 通道激活，发送 PartitionRequest...");
            // 模拟 Flink: 发送 PartitionRequest，携带初始 Credit
            // 在真实 Flink 中，这一步通常由 PartitionRequestClientFactory 触发
            ctx.writeAndFlush(new NettyMessage.PartitionRequest(0, 10));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
            if (msg instanceof NettyMessage.BufferResponse) {
                NettyMessage.BufferResponse response = (NettyMessage.BufferResponse) msg;

                // 将接收到的 Buffer 传递给 InputGate
                inputGate.onBuffer(response.buffer);

                // 模拟 Credit 机制：每收到一个 Buffer，就回送一个 AddCredit
                // 这样保证 Server 端知道我们有能力继续接收
                ctx.writeAndFlush(new NettyMessage.AddCredit(1));

            } else if (msg instanceof NettyMessage.ErrorResponse) {
                log.error("[Client] 收到服务端错误: {}", ((NettyMessage.ErrorResponse) msg).message);
                ctx.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[Client] Handler 异常", cause);
            ctx.close();
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.PartitionRequestServerHandler
     * * 职责：
     * 1. 处理 PartitionRequest。
     * 2. 处理 AddCredit。
     * 3. 持有 OutboundQueue (这里简化为直接启动线程生成数据) 来发送 BufferResponse。
     */
    @Slf4j
    public static class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

        // 模拟 Flink 的 Reader 上下文
        private volatile boolean isStreaming = false;
        private final AtomicInteger availableCredit = new AtomicInteger(0);

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
            if (msg instanceof NettyMessage.PartitionRequest) {
                NettyMessage.PartitionRequest req = (NettyMessage.PartitionRequest) msg;
                log.info("[Server] 收到分区请求 Partition={}, InitialCredit={}", req.partitionId, req.credit);

                availableCredit.set(req.credit);

                if (!isStreaming) {
                    isStreaming = true;
                    startDataGenerator(ctx);
                }

            } else if (msg instanceof NettyMessage.AddCredit) {
                int delta = ((NettyMessage.AddCredit) msg).credit;
                int newCredit = availableCredit.addAndGet(delta);
                // log.debug("[Server] 收到 Credit 补充: +{}, 当前: {}", delta, newCredit);
            }
        }

        /**
         * 模拟 Flink 的 SubpartitionView 读取逻辑。
         * 在真实 Flink 中，这里是注册一个 Listener，当 Buffer 产生时触发发送。
         * 这里我们用线程模拟"生产-发送"循环，并加入了简单的 Credit 流控检查。
         */
        private void startDataGenerator(ChannelHandlerContext ctx) {
            new Thread(() -> {
                Random random = new Random();
                int seq = 0;
                try {
                    while (ctx.channel().isActive()) {
                        // 简单的流控：如果没有 Credit，就等待
                        if (availableCredit.get() <= 0) {
                            Thread.sleep(10);
                            continue;
                        }

                        // 模拟数据生产耗时
                        int sleep = random.nextInt(100) < 5 ? 200 : 10;
                        TimeUnit.MILLISECONDS.sleep(sleep);

                        // 构造数据
                        String payload = "Flink-Netty-Record-" + (++seq);
                        MiniFlink.NetworkBuffer buffer = new MiniFlink.NetworkBuffer(payload);

                        // 消耗 Credit
                        availableCredit.decrementAndGet();

                        // 发送
                        ctx.writeAndFlush(new NettyMessage.BufferResponse(buffer, seq, 0));
                    }
                } catch (Exception e) {
                    log.error("[Server] 数据生成线程异常", e);
                }
            }, "MiniFlink-DataProducer").start();
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.NettyProtocol
     * 工厂类，负责组装 Server 和 Client 的 Pipeline。
     */
    public static class NettyProtocol {

        private final MiniFlink.MiniInputGate inputGate;

        public NettyProtocol(MiniFlink.MiniInputGate inputGate) {
            this.inputGate = inputGate;
        }

        public ChannelHandler[] getServerChannelHandlers() {
            return new ChannelHandler[] {
                    // 1. 编码器 (Outbound)
                    new NettyMessage.NettyMessageEncoder(),
                    // 2. 解码器 (Inbound) - 服务端也要接收 Request，所以也需要 Decoder
                    // 为简化，服务端使用简单的 DecoderDelegate (实际 Flink 中服务端主要接收 Request)
                    new NettyMessageClientDecoderDelegate(new NetworkBufferAllocator.SimpleAllocator()),
                    // 3. 业务 Handler
                    new PartitionRequestServerHandler()
            };
        }

        public ChannelHandler[] getClientChannelHandlers() {
            return new ChannelHandler[] {
                    // 1. 编码器 (Outbound)
                    new NettyMessage.NettyMessageEncoder(),
                    // 2. 解码器 (Inbound) - 包含分层解码逻辑
                    new NettyMessageClientDecoderDelegate(new NetworkBufferAllocator.SimpleAllocator()),
                    // 3. 业务 Handler
                    new CreditBasedPartitionRequestClientHandler(inputGate)
            };
        }
    }

    /**
     * 对应 Flink 源码中的 InputGate。
     * 修改：接收 NetworkBuffer 而不是 String。
     */
    @Slf4j
    public static class MiniInputGate {

        private final Queue<NetworkBuffer> queue = new ArrayDeque<>();
        private final ReentrantLock lock = new ReentrantLock();

        // 可用性 Future，用于 Mailbox 机制下的唤醒
        private CompletableFuture<Void> availabilityFuture = new CompletableFuture<>();

        /**
         * [Netty 线程调用] 接收网络层传来的 Buffer
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
                    // 队列空了，重置 future，表示"目前不可用"
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
        public CompletableFuture<Void> getAvailableFuture() {
            lock.lock();
            try {
                return availabilityFuture;
            } finally {
                lock.unlock();
            }
        }
    }

    @Slf4j
    public static class NettyServer {
        private final int port;
        private final NettyProtocol protocol;
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;

        public NettyServer(int port, NettyProtocol protocol) {
            this.port = port;
            this.protocol = protocol;
        }

        public void start() {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(protocol.getServerChannelHandlers());
                            }
                        })
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);

                b.bind(port).sync();
                log.info("=== MiniFlink Netty Server 启动在端口 {} ===", port);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void shutdown() {
            if (bossGroup != null) bossGroup.shutdownGracefully();
            if (workerGroup != null) workerGroup.shutdownGracefully();
        }
    }

    @Slf4j
    public static class NettyClient {
        private final String host;
        private final int port;
        private final NettyProtocol protocol;
        private EventLoopGroup group;

        public NettyClient(String host, int port, NettyProtocol protocol) {
            this.host = host;
            this.port = port;
            this.protocol = protocol;
        }

        public void start() {
            group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(protocol.getClientChannelHandlers());
                            }
                        });

                ChannelFuture f = b.connect(host, port).sync();
                log.info("=== MiniFlink Netty Client 已连接到 {}:{} ===", host, port);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void shutdown() {
            if (group != null) {
                group.shutdownGracefully();
            }
        }
    }

    /**
     * 对应 Flink 源码中的 StreamOneInputProcessor。
     * 它是 MailboxDefaultAction 的具体实现者。
     */
    @Slf4j
    static class StreamInputProcessor implements MailboxDefaultAction {

        private final MiniInputGate inputGate;
        private final DataOutput output;

        public interface DataOutput {
            void processRecord(String record);
        }

        public StreamInputProcessor(MiniInputGate inputGate, DataOutput output) {
            this.inputGate = inputGate;
            this.output = output;
        }

        @Override
        public void runDefaultAction(Controller controller) {
            // 1. 尝试从 InputGate 拿数据 (Buffer)
            NetworkBuffer buffer = inputGate.pollNext();

            if (buffer != null) {
                // A. 有数据，处理
                // 在这里做简单的反序列化 (Buffer -> String)
                String record = buffer.toStringContent();
                output.processRecord(record);
            } else {
                // B. 没数据了 (InputGate 空)
                // 1. 获取 InputGate 的"可用性凭证" (Future)
                CompletableFuture<Void> availableFuture = inputGate.getAvailableFuture();

                if (availableFuture.isDone()) {
                    return;
                }

                // 2. 告诉 MailboxProcessor：暂停默认动作
                controller.suspendDefaultAction();

                // 3. 当 Netty 线程放入数据并 complete future 时，触发 resume
                availableFuture.thenRun(() -> {
                    // 注意：这里是在 Netty 线程运行，跨线程调用 resume
                    ((MailboxProcessor) controller).resumeDefaultAction();
                });
            }
        }
    }

    /**
     * 具体的业务 Task。
     * 修改点：
     * 1. 在构造函数中注册了一个周期性的定时器。
     * 2. 演示了标准的 Flink Mailbox 定时器模式：Timer线程 -> Mailbox -> Main线程。
     */
    @Slf4j
    static class CounterStreamTask extends StreamTask implements StreamInputProcessor.DataOutput {

        private final StreamInputProcessor inputProcessor;
        private long recordCount = 0;

        public CounterStreamTask(MiniInputGate inputGate) {
            super();
            this.inputProcessor = new StreamInputProcessor(inputGate, this);

            // [新增] 注册第一个定时器：1秒后触发
            registerPeriodicTimer(1000);
        }

        /**
         * [新增] 注册周期性定时器的演示方法
         */
        private void registerPeriodicTimer(long delayMs) {
            long triggerTime = timerService.getCurrentProcessingTime() + delayMs;

            // 1. 向 TimerService 注册 (这是在后台线程池触发)
            timerService.registerTimer(triggerTime, timestamp -> {

                // 2. [关键] 定时器触发时，我们身处 "Flink-System-Timer-Service" 线程。
                // 绝对不能直接操作 recordCount 等状态！
                // 必须通过 mailboxExecutor 将逻辑"邮寄"回主线程执行。
                mainMailboxExecutor.execute(() -> {
                    // 这里是主线程，安全地访问状态
                    onTimer(timestamp);
                }, "PeriodicTimer-" + timestamp);
            });
        }

        /**
         * [新增] 定时器具体的业务逻辑（运行在主线程）
         */
        private void onTimer(long timestamp) {
            log.info(" >>> [Timer Fired] timestamp: {}, 当前处理条数: {}", timestamp, recordCount);

            // 注册下一次定时器 (模拟周期性)
            registerPeriodicTimer(1000);
        }

        @Override
        public void runDefaultAction(Controller controller) {
            inputProcessor.runDefaultAction(controller);
        }

        @Override
        public void processRecord(String record) {
            this.recordCount++;
            if (recordCount % 10 == 0) {
                log.info("Task 处理进度: {} 条", recordCount);
            }
        }

        @Override
        public void performCheckpoint(long checkpointId) {
            log.info(" >>> [Checkpoint Starting] ID: {}, 当前状态值: {}", checkpointId, recordCount);
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info(" <<< [Checkpoint Finished] ID: {} 完成", checkpointId);
        }
    }

    /**
     * 模拟 JobMaster 定时触发 Checkpoint。
     * 这里的核心是：它不再自己去读取 Task 状态，而是往 Task 的邮箱里扔一个"命令"。
     */
    @Slf4j
    static class CheckpointScheduler extends Thread {

        private final MailboxExecutor taskMailboxExecutor;
        private final StreamTask task;
        private volatile boolean running = true;

        public CheckpointScheduler(StreamTask task) {
            super("Checkpoint-Timer");
            this.task = task;
            // 获取高优先级的执行器 (Checkpoint 优先级 > 数据处理)
            this.taskMailboxExecutor = task.getControlMailboxExecutor();
        }

        @Override
        public void run() {
            long checkpointId = 0;
            while (running) {
                try {
                    TimeUnit.MILLISECONDS.sleep(2000); // 每2秒触发一次
                    long id = ++checkpointId;

                    log.info("[JM] 触发 Checkpoint {}", id);

                    // === 关键点 ===
                    // 我们不在这里调用 task.performCheckpoint()，因为那会导致线程不安全。
                    // 我们创建一个 Mail (Lambda)，扔给 Task 线程自己去跑。
                    taskMailboxExecutor.execute(() -> task.performCheckpoint(id), "Checkpoint-" + id);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public void shutdown() {
            running = false;
            this.interrupt();
        }
    }

    /**
     * 新的启动类，集成了 Netty 网络通信模块。
     * 流程：
     * 1. 启动 Netty Server (模拟上游 TM)
     * 2. 启动 Netty Client (模拟本 TM 网络层)，并关联 InputGate
     * 3. 启动 Task 主线程，从 InputGate 消费 Netty 传来的数据
     */
    @Slf4j
    static class EntryPoint {

        public static void main(String[] args) {
            log.info("=== MiniFlink (Netty Version) 启动 ===");

            int port = 9091;

            MiniInputGate inputGate = new MiniInputGate();
            NettyProtocol protocol = new NettyProtocol(inputGate);

            NettyServer server = new NettyServer(port, protocol);
            new Thread(server::start).start();
            NettyClient client = new NettyClient("127.0.0.1", port, protocol);
            client.start();

            // 4. 构建 Task
            try {
                log.info("[Main] 构建 Task 环境...");

                // [修改点] 这里直接使用功能更全的 CounterStreamTask (包含 Timer 演示)
                // 而不是使用 MyNettyStreamTask
                CounterStreamTask task = new CounterStreamTask(inputGate);

                // 启动 Checkpoint 调度器
                CheckpointScheduler cpScheduler = new CheckpointScheduler(task);
                cpScheduler.start();

                // 5. 启动 Task 主循环
                log.info("[Main] 开始执行 Task invoke...");
                task.invoke();

            } catch (Exception e) {
                log.error("Task 运行失败", e);
            } finally {
                client.shutdown();
                server.shutdown();
            }
        }
    }

}
