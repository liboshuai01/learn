package cn.liboshuai.flink;


import java.util.concurrent.CompletableFuture;

public class StreamInputProcessor implements MailboxDefaultAction{

    private final MiniInputGate inputGate;
    private final DataOutput output;

    public StreamInputProcessor(MiniInputGate inputGate, DataOutput output) {
        this.inputGate = inputGate;
        this.output = output;
    }

    @Override
    public void runDefaultAction(Controller controller) throws Exception {
        // 1. 尝试从 InputGate 拿数据（Buffer）
        NetworkBuffer buffer = inputGate.pollNext();
        if (buffer != null) {
            // A. 有数据，处理
            // 在这里做简单的反序列化（Buffer -> String）
            String record = buffer.toStringContent();
            output.processRecord(record);
        } else {
            // B. 没数据了（InputGate 空）
            // 1. 获取 InputGate 的“可用性凭证”（Future）
            CompletableFuture<Void> availabilityFuture = inputGate.getAvailabilityFuture();
            if (availabilityFuture.isDone()) {
                return;
            }
            // 2. 告诉 MailboxProcessor: 暂停默认动作
            controller.suspendDefaultAction();
            // 3. 当 Netty 线程放入数据并 complete future 时，触发 resume
            availabilityFuture.thenRun(() -> {
                // 注意：这里是在 Netty 线程中运行，跨线程调用 resume
                ((MailboxProcessor) controller).resumeDefaultAction();
            });
        }
    }
}
