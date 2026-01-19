package com.liboshuai.demo.netty.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.liboshuai.demo.netty.common.RequestData;
import com.liboshuai.demo.netty.common.ResponseData;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ClientMain {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        int port = 25522;
        EventLoopGroup group = new NioEventLoopGroup();
        ResponseFutureManager responseFutureManager = new ResponseFutureManager();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            // 添加用于消息帧和编解码的处理器
                            p.addLast(new LineBasedFrameDecoder(1024)); // 处理消息边界
                            p.addLast(new StringDecoder());
                            p.addLast(new StringEncoder());
                            p.addLast(new ClientHandler(responseFutureManager));
                        }
                    });

            // 启动客户端
            ChannelFuture f = b.connect(host, port).sync();
            log.info("Netty 客户端已连接到 {}:{}", host, port);
            Channel channel = f.channel();

            ObjectMapper objectMapper = new ObjectMapper();

            // 从控制台读取用户输入
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while (true) {
                    log.info(">>> 请输入要发送的消息，或输入 'exit' 退出 <<<");
                    line = reader.readLine();
                    if (line == null || "exit".equalsIgnoreCase(line.trim())) {
                        break;
                    }

                    // 创建一个带唯一 ID 的请求
                    String requestId = UUID.randomUUID().toString();
                    RequestData request = new RequestData(requestId, line);
                    String requestJson = objectMapper.writeValueAsString(request) + "\n";

                    // 为响应准备 CompletableFuture
                    CompletableFuture<ResponseData> future = new CompletableFuture<>();
                    responseFutureManager.registerFuture(requestId, future);

                    log.info("正在向服务器发送消息: [{}], 等待异步响应...", line);
                    channel.writeAndFlush(requestJson);

                    // 异步处理响应
                    final Duration timeout = Duration.ofSeconds(5);
                    future.whenComplete((response, error) -> {
                        if (error != null) {
                            if (error instanceof TimeoutException) {
                                log.error("请求超时! 服务器未在 {} 秒内响应。", timeout.getSeconds());
                            } else {
                                log.error("请求因错误而失败", error);
                            }
                        } else {
                            log.info("收到来自服务器的异步响应: [{}]", response.getResponse());
                        }
                    });
                }
            }

            // 等待连接关闭
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("客户端发生错误", e);
        } finally {
            log.info("Netty 客户端正在关闭...");
            group.shutdownGracefully();
        }
    }
}