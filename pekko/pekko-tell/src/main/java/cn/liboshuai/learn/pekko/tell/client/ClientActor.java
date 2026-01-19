package cn.liboshuai.learn.pekko.tell.client;

import cn.liboshuai.learn.pekko.tell.common.RequestData;
import cn.liboshuai.learn.pekko.tell.common.ResponseData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.Props;

@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class ClientActor extends AbstractActor {

    private ActorSelection serverSelection;

    /**
     * pekko://serverSystem@127.0.0.1:25520/user/serverActor
     * @param serverPath 表示服务端的引用地址 [pekko://serverSystem@127.0.0.1:25520/user/serverActor]
     */
    public ClientActor(String serverPath) {
        this.serverSelection = getContext().actorSelection(serverPath);
    }

    /**
     * 如何创建 ClientActor 的配方，给 ActorSystem 使用的
     * @param serverPath 表示服务端的引用地址 [pekko://serverSystem@127.0.0.1:25520/user/serverActor]
     * @return 创建 ClientActor 的配方
     */
    public static Props props(String serverPath) {
        return Props.create(ClientActor.class, () -> new ClientActor(serverPath));
    }

    /**
     * 当 clientActor 接收到消息时，会调用此方法
     */
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // string 类型的数据是咱们自己在控制台发送的数据
                .match(String.class, data -> {
                    // 接收到控制台发送的数据后，咱们要将数据发送到服务端
                    RequestData requestData = new RequestData(data);
                    serverSelection.tell(requestData, getSelf());
                    log.info("接收到控制台数据 [{}]，发送给服务端", data);
                })
                // ResponseData 类型的数据是服务端发送过来的数据类型，匹配成功
                .match(ResponseData.class, data -> {
                    log.info("接收到服务端数据 [{}]", data.getData());
                })
                .matchAny(data -> {
                    log.warn("接收到未知类型的数据，内容为: [{}]", data);
                })
                .build();
    }
}
