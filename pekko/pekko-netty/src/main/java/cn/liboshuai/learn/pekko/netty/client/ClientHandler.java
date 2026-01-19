package cn.liboshuai.learn.pekko.netty.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import cn.liboshuai.learn.pekko.netty.common.ResponseData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ClientHandler extends SimpleChannelInboundHandler<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ResponseFutureManager responseFutureManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        log.debug("收到来自服务器的原始响应: {}", msg);

        ResponseData response = objectMapper.readValue(msg, ResponseData.class);
        responseFutureManager.completeFuture(response.getRequestId(), response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("ClientHandler 中发生错误", cause);
        ctx.close();
    }
}