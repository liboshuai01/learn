package com.liboshuai.demo.server;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import java.io.IOException;

@Slf4j
public class ServerSystem {
    public static void main(String[] args) {
        ActorSystem serverSystem = ActorSystem.create("serverSystem", ConfigFactory.load("server.conf"));
        ActorRef serveractorRef = serverSystem.actorOf(ServerActor.props(), "serverActor");

        log.info("服务端 actor 已经准备就绪，完整的请求地址为: [{}]", serveractorRef.path());

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
