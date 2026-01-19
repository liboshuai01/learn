package cn.liboshuai.learn.pekko.tell.client;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Slf4j
public class ClientMain {
    public static void main(String[] args) {
        // 通过加载 client.conf 创建 actorSystem 对象
        ActorSystem clientSystem = ActorSystem.create("clientSystem", ConfigFactory.load("client.conf"));

        // 表示 服务端 actor 的引用路径
        String serverPath = "pekko://serverSystem@127.0.0.1:25520/user/serverActor";
        // 通过 actorSystem 创建出我们需要的客户端 actor，返回得到一个客户端actor的引用
        ActorRef clientActorRef = clientSystem.actorOf(ClientActor.props(serverPath), "clientActor");

//        clientActorRef.tell("测试", ActorRef.noSender());
        log.info("客户端 actor 已经准备就绪，完整路径为: [{}]", clientActorRef.path());
        log.info(">>> 请在控制台输入需要发送的文本消息（输入 exit 表示退出）<<<");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine())  != null) {
                if ("exit".equalsIgnoreCase(line)) {
                    break;
                }
                clientActorRef.tell(line, ActorRef.noSender());
            }
        } catch (IOException e) {
            log.error("等待输入时发生的错误: ", e);
        } finally {
            log.info("客户端正在关闭......");
            clientSystem.terminate();
        }
    }
}
