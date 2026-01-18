package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntryPoint {
    public static void main(String[] args) {
        log.info("=== MiniFlink (Netty Version) 启动 ===");

        int port = 9091;

        // 1. 初始化 InputGate
        MiniInputGate inputGate = new MiniInputGate();
        // 2. 启动服务端（模拟上游 TM）
        NettyServer server = new NettyServer(port);
        new Thread(server::start).start();
        // 3. 启动客户端（模拟本 TM 网络层）
        NettyClient client = new NettyClient("127.0.0.1", port, inputGate);
        client.start();

        // 4. 构建 Task
        try {
            log.info("[Main] 构建 Task 环境...");
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
