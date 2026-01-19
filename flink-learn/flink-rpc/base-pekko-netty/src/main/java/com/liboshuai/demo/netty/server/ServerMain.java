package com.liboshuai.demo.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerMain {
    public static void main(String[] args) {
        // 1. 创建两个事件循环组，用于 boss 和 worker 线程
        EventLoopGroup bossGroup = new NioEventLoopGroup(1); // 用于接受连接
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // 用于处理已接受连接的 I/O

        try {
            // 2. 创建服务器引导程序
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // 指定使用 NIO 进行传输
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            // 向管道中添加处理器
                            // 注意: Netty 的 StringEncoder/Decoder 比较简单，对于 JSON 我们需要自定义处理器
                            p.addLast(new StringDecoder());
                            p.addLast(new StringEncoder());
                            // 添加我们的自定义业务逻辑处理器
                            p.addLast(new ServerHandler());
                        }
                    });

            // 4. 绑定端口并开始接受传入连接
            int port = 25522;
            ChannelFuture f = b.bind(port).sync();
            log.info("Netty 服务端已在端口 {} 启动", port);

            // 5. 等待服务器 socket 关闭
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("服务端被中断", e);
            Thread.currentThread().interrupt();
        } finally {
            // 6. 优雅地关闭所有事件循环组以终止所有线程
            log.info("Netty 服务端正在关闭...");
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}