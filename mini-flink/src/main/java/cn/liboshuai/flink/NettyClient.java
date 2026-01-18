package cn.liboshuai.flink;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

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
            ChannelFuture f = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<NioSocketChannel>() {
                        @Override
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new NettyMessage.MessageDecoder());
                            ch.pipeline().addLast(new NettyMessage.MessageEncoder());
                            ch.pipeline().addLast(new NettyClientHandler(inputGate));
                        }
                    }).connect(host, port).sync();
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
