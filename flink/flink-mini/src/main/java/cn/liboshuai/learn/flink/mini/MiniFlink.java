package cn.liboshuai.learn.flink.mini;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
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
    static class NetworkBuffer {

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
     * 参考 org.apache.flink.runtime.io.network.netty.NettyMessage
     * 定义 MiniFlink 的网络传输协议。
     * 协议格式: [Frame Length (4 bytes)] [Magic Number (4 bytes)] [Msg ID (1 byte)] [Body...]
     */
    static abstract class NettyMessage {

        public static final int MAGIC_NUMBER = 0xBADC0FFE; // Flink 经典魔数
        public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // Length + Magic + ID

        // 消息类型 ID
        public static final byte ID_PARTITION_REQUEST = 1;
        public static final byte ID_BUFFER_RESPONSE = 2;

        // --- 消息子类定义 ---

        /**
         * 客户端向服务端请求分区数据 (握手)
         */
        public static class PartitionRequest extends NettyMessage {
            public final int partitionId;
            public final int credit; // 模拟信用分

            public PartitionRequest(int partitionId, int credit) {
                this.partitionId = partitionId;
                this.credit = credit;
            }
        }

        /**
         * 服务端向客户端发送数据
         */
        public static class BufferResponse extends NettyMessage {
            public final NetworkBuffer buffer;

            public BufferResponse(NetworkBuffer buffer) {
                this.buffer = buffer;
            }
        }

        // --- 编解码器 (Encoder / Decoder) ---

        /**
         * 编码器：将对象转换为 ByteBuf
         */
        public static class MessageEncoder extends MessageToByteEncoder<NettyMessage> {
            @Override
            protected void encode(ChannelHandlerContext ctx, NettyMessage msg, ByteBuf out) {
                // 1. 预留 4 字节写 Frame Length
                int startIndex = out.writerIndex();
                out.writeInt(0);

                // 2. 写 Magic Number
                out.writeInt(MAGIC_NUMBER);

                // 3. 写 Body
                if (msg instanceof PartitionRequest) {
                    out.writeByte(ID_PARTITION_REQUEST);
                    PartitionRequest req = (PartitionRequest) msg;
                    out.writeInt(req.partitionId);
                    out.writeInt(req.credit);
                } else if (msg instanceof BufferResponse) {
                    out.writeByte(ID_BUFFER_RESPONSE);
                    BufferResponse resp = (BufferResponse) msg;
                    byte[] data = resp.buffer.getBytes();
                    out.writeInt(data.length);
                    out.writeBytes(data);
                } else {
                    throw new IllegalArgumentException("未知的消息类型: " + msg.getClass());
                }

                // 4. 回填 Frame Length (当前 writeIndex - startIndex - 4字节长度字段本身)
                int endIndex = out.writerIndex();
                out.setInt(startIndex, endIndex - startIndex - 4);
            }
        }

        /**
         * 解码器：基于长度帧解码 (解决粘包拆包问题)
         */
        public static class MessageDecoder extends LengthFieldBasedFrameDecoder {

            public MessageDecoder() {
                // maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip
                super(Integer.MAX_VALUE, 0, 4, 0, 4);
            }

            @Override
            protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
                ByteBuf frame = (ByteBuf) super.decode(ctx, in);
                if (frame == null) {
                    return null;
                }

                try {
                    // 验证魔数
                    int magic = frame.readInt();
                    if (magic != MAGIC_NUMBER) {
                        throw new IllegalStateException("魔数错误，网络流可能已损坏");
                    }

                    // 读取消息 ID
                    byte msgId = frame.readByte();
                    switch (msgId) {
                        case ID_PARTITION_REQUEST:
                            int partId = frame.readInt();
                            int credit = frame.readInt();
                            return new PartitionRequest(partId, credit);

                        case ID_BUFFER_RESPONSE:
                            int dataLen = frame.readInt();
                            byte[] data = new byte[dataLen];
                            frame.readBytes(data);
                            return new BufferResponse(new NetworkBuffer(data));

                        default:
                            throw new IllegalStateException("收到未知消息 ID: " + msgId);
                    }
                } finally {
                    frame.release();
                }
            }
        }
    }

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.PartitionRequestServerHandler
     * 服务端 Handler，处理客户端发来的 PartitionRequest。
     */
    @Slf4j
    static class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
            if (msg instanceof NettyMessage.PartitionRequest) {
                NettyMessage.PartitionRequest req = (NettyMessage.PartitionRequest) msg;
                log.info("[Server] 收到分区请求: PartitionId={}, InitialCredit={}", req.partitionId, req.credit);

                // 模拟：收到请求后，启动一个后台线程不断产生数据并发给客户端
                // 在 Flink 中，这里会创建一个 ViewReader 去读取 ResultSubpartition
                startDataGenerator(ctx);
            }
        }

        private void startDataGenerator(ChannelHandlerContext ctx) {
            new Thread(() -> {
                log.info("[Server] 开始向客户端发送数据流...");
                Random random = new Random();
                int seq = 0;
                try {
                    while (ctx.channel().isActive()) {
                        // 模拟生产延迟
                        int sleep = random.nextInt(100) < 5 ? 500 : 20;
                        TimeUnit.MILLISECONDS.sleep(sleep);

                        // 构造数据
                        String payload = "Netty-Record-" + (++seq);
                        NetworkBuffer buffer = new NetworkBuffer(payload);

                        // 封装为 BufferResponse 发送
                        ctx.writeAndFlush(new NettyMessage.BufferResponse(buffer));
                    }
                } catch (Exception e) {
                    log.error("[Server] 数据生成线程异常", e);
                }
                log.info("[Server] 数据发送结束");
            }, "MiniFlink-DataProducer").start();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[Server] 连接异常", cause);
            ctx.close();
        }
    }

    /**
     * 对应 Flink 源码中的 InputGate。
     * 修改：接收 NetworkBuffer 而不是 String。
     */
    @Slf4j
    static class MiniInputGate {

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

    /**
     * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler
     * 客户端 Handler，处理服务端推送过来的数据 (BufferResponse)。
     */
    @Slf4j
    static class NettyClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

        private final MiniInputGate inputGate;

        public NettyClientHandler(MiniInputGate inputGate) {
            this.inputGate = inputGate;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("[Client] 连接建立，发送 PartitionRequest...");
            // 模拟：连接建立后立即请求分区 0，带上初始 Credit 2
            ctx.writeAndFlush(new NettyMessage.PartitionRequest(0, 2));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
            if (msg instanceof NettyMessage.BufferResponse) {
                NettyMessage.BufferResponse response = (NettyMessage.BufferResponse) msg;

                // 将网络层收到的 Buffer 转交给 InputGate
                // 在 Flink 中，这里会根据 inputChannelId 找到对应的 RemoteInputChannel 并 onBuffer()
                inputGate.onBuffer(response.buffer);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[Client] 异常", cause);
            ctx.close();
        }
    }

    /**
     * 模拟 Flink 的 NettyServer
     */
    @Slf4j
    static class NettyServer {

        private final int port;
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;

        public NettyServer(int port) {
            this.port = port;
        }

        public void start() {
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 添加编解码器和业务 Handler
                        ch.pipeline().addLast(new NettyMessage.MessageDecoder());
                        ch.pipeline().addLast(new NettyMessage.MessageEncoder());
                        ch.pipeline().addLast(new PartitionRequestServerHandler());
                    }
                }).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);

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

    /**
     * 模拟 Flink 的 NettyConnectionManager / NettyClient
     */
    @Slf4j
    static class NettyClient {

        private final String host;
        private final int port;
        private final MiniInputGate inputGate;
        private EventLoopGroup group;

        public NettyClient(String host, int port, MiniInputGate inputGate) {
            this.host = host;
            this.port = port;
            this.inputGate = inputGate;
        }

        public void start() {
            group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 客户端管线：解码 -> 编码 -> ClientHandler(持有 InputGate)
                        ch.pipeline().addLast(new NettyMessage.MessageDecoder());
                        ch.pipeline().addLast(new NettyMessage.MessageEncoder());
                        ch.pipeline().addLast(new NettyClientHandler(inputGate));
                    }
                });

                ChannelFuture f = b.connect(host, port).sync();
                log.info("=== MiniFlink Netty Client 已连接到 {}:{} ===", host, port);
                // 这里不阻塞等待 close，因为我们需要主线程去运行 Task
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

            // 1. 初始化 InputGate
            MiniInputGate inputGate = new MiniInputGate();

            // 2. 启动服务端 (模拟上游 TM)
            NettyServer server = new NettyServer(port);
            new Thread(server::start).start();

            // 3. 启动客户端 (模拟本 TM 网络层)
            NettyClient client = new NettyClient("127.0.0.1", port, inputGate);
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
