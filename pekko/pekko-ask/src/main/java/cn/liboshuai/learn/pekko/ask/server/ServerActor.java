package cn.liboshuai.learn.pekko.ask.server;

import cn.liboshuai.learn.pekko.ask.common.RequestData;
import cn.liboshuai.learn.pekko.ask.common.ResponseData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;

@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class ServerActor extends AbstractActor {

    public static Props props() {
        return Props.create(ServerActor.class, ServerActor::new);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RequestData.class, requestData -> {
                    log.info("接收到来自客户端的数据: [{}]", requestData.getMessage());
                    ActorRef sender = getSender();
                    ResponseData responseData = new ResponseData(String.format("服务端已经收到内容为[%s]的消息了！", requestData.getMessage()));
                    sender.tell(responseData, getSelf());
                    log.info("回显给客户端的数据: [{}]", responseData.getMessage());
                })
                .build();
    }
}
