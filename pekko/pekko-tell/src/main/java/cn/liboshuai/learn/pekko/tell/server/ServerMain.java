package cn.liboshuai.learn.pekko.tell.server;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import java.io.IOException;

@Slf4j
public class ServerMain {
    public static void main(String[] args) {
        // 通过加载 server.conf 配置文件创建服务端的 actorSystem
        ActorSystem serverSystem = ActorSystem.create("serverSystem", ConfigFactory.load("server.conf"));

        ActorRef serverActorRef = serverSystem.actorOf(ServerActor.props(), "serverActor");

        // pekko://serverSystem@127.0.0.1:25520/user/serverActor
        log.info("服务端 actor 已经准备就绪，完整路径为: {}", serverActorRef.path());

        log.info(">>> 按回车键退出服务端 <<<");
        try {
            int ignored = System.in.read();
        } catch (IOException e) {
            log.error("等待输入时发生错误", e);
        }finally {
            serverSystem.terminate();
        }
    }
}
