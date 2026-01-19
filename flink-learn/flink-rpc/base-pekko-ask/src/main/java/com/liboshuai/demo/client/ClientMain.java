package com.liboshuai.demo.client;

import com.liboshuai.demo.common.RequestData;
import com.liboshuai.demo.common.ResponseData;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.pattern.Patterns;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ClientMain {
    public static void main(String[] args) {
        ActorSystem clientSystem = ActorSystem.create("clientSystem", ConfigFactory.load("client.conf"));

        String serverPath = "pekko://serverSystem@127.0.0.1:25522/user/serverActor";
        ActorSelection serverActorSelection = clientSystem.actorSelection(serverPath);

//        serverActorSelection.tell("测试", ActorRef.noSender());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            final Duration timeout = Duration.ofSeconds(5);
            while (true) {
                log.info(">>> 请在控制台输入要发送的消息, 输入 'exit' 退出 <<<");
                line = reader.readLine();

                if (line == null || "exit".equalsIgnoreCase(line.trim())) {
                    break;
                }
                RequestData requestData = new RequestData(line);

                // 这一步请求是异步的，这一步行代码执行完成后，并不代表请求已经发送到服务端了，更不代表服务的数据已经响应给客户端了
                CompletionStage<Object> completionStage = Patterns.ask(serverActorSelection, requestData, timeout);

                log.info("已向服务端发送消息: [{}], 等待异步响应...", line);

                // 只是注册一下，并不会阻塞主线程代码，执行很快。等服务端响应完成后，这里的回调方法才会被执行
                completionStage.whenComplete((response, throwable) -> {
                    if (throwable == null) {
                        ResponseData responseData = (ResponseData) response;
                        log.info("服务端返回数据 [{}]", responseData.getMessage());
                    } else {
                        if (throwable instanceof TimeoutException) {
                            log.error("请求超时！服务端未在 {} 秒内完成响应。", timeout.getSeconds());
                        } else {
                            log.error("请求出现未知异常，未能完成", throwable);
                        }
                    }
                });
            }
        } catch (IOException e) {
            log.error("等待输入时发生错误。", e);
        } finally {
            log.info("客户端正在关闭......");
            clientSystem.terminate();
        }

    }
}
