package cn.liboshuai.learn.pekko.tell.server;

import cn.liboshuai.learn.pekko.tell.common.RequestData;
import cn.liboshuai.learn.pekko.tell.common.ResponseData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.AbstractActor;
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
                .match(RequestData.class, data -> {
                    log.info("接收到客户端发送的数据: [{}]", data.getData());
                    String responseString = String.format("已收到客户端发送来的信息: [%s]",  data.getData());
                    ResponseData responseData = new ResponseData(responseString);
                    getSender().tell(responseData,  getSelf());
                    log.info("回显给客户端发送的数据: [{}]", responseString);
                })
                .matchAny(data -> {
                    log.warn("接收到未知类型的数据，内容为: [{}]", data);
                })
                .build();
    }
}
