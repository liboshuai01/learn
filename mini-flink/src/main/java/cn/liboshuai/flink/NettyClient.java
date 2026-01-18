package cn.liboshuai.flink;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * 模拟 Flink 的 NettyConnectionManager / NettyClient
 */
@Slf4j
public class NettyClient {

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
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 客户端管线：解码 -> 编码 -> ClientHandler(持有 InputGate)
                            ch.pipeline().addLast(new NettyMessage.MessageDecoder());
                            ch.pipeline().addLast(new NettyMessage.MessageEncoder());
                            ch.pipeline().addLast(new NettyClientHandler(inputGate));
                        }
                    });

            b.connect(host, port).sync();
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