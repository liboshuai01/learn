package cn.liboshuai.flink;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 对应 Flink 的 org.apache.flink.runtime.io.network.netty.PartitionRequestServerHandler
 * 服务端 Handler，处理客户端发来的 PartitionRequest。
 */
@Slf4j
public class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

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