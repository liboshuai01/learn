package cn.liboshuai.flink;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyClientHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private final MiniInputGate inputGate;

    public NettyClientHandler(MiniInputGate inputGate) {
        this.inputGate = inputGate;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("[Client] 连接建立，发送 PartitionRequest...");
        // 模拟：连接建立后立即请求分区 0,带上初始 Credit 2
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("[Client] 异常", cause);
        ctx.close();
    }
}
