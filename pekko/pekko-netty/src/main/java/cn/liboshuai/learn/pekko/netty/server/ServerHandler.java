package cn.liboshuai.learn.pekko.netty.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import cn.liboshuai.learn.pekko.netty.common.RequestData;
import cn.liboshuai.learn.pekko.netty.common.ResponseData;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerHandler extends SimpleChannelInboundHandler<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        log.info("收到来自客户端的原始消息: {}", msg);

        // 1. 将 JSON 字符串反序列化为 RequestData 对象
        RequestData requestData = objectMapper.readValue(msg, RequestData.class);
        log.info("反序列化后的请求: [{}], 内容: [{}]", requestData.getRequestId(), requestData.getData());

        // 2. 业务逻辑: 创建一个响应消息
        String replyMessage = String.format("消息 [%s] 已收到！", requestData.getData());
        ResponseData response = new ResponseData(requestData.getRequestId(), replyMessage);

        log.info("正在向客户端响应: [{}], 内容: [{}]", response.getRequestId(), response.getResponse());

        // 3. 将 ResponseData 对象序列化为 JSON 字符串并发送回去
        String responseJson = objectMapper.writeValueAsString(response) + "\n"; // 添加换行符作为分隔符
        ctx.writeAndFlush(responseJson);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("ServerHandler 中发生错误", cause);
        ctx.close();
    }
}