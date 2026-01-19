package cn.liboshuai.learn.pekko.netty.client;

import cn.liboshuai.learn.pekko.netty.common.ResponseData;

import java.util.Map;
import java.util.concurrent.*;

public class ResponseFutureManager {

    private final Map<String, CompletableFuture<ResponseData>> futureMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);

    public void registerFuture(String requestId, CompletableFuture<ResponseData> future) {
        futureMap.put(requestId, future);

        // 安排一个超时检查
        timeoutScheduler.schedule(() -> {
            if (!future.isDone()) {
                future.completeExceptionally(new TimeoutException("请求超时"));
                futureMap.remove(requestId);
            }
        }, 5, TimeUnit.SECONDS);
    }

    public void completeFuture(String requestId, ResponseData response) {
        CompletableFuture<ResponseData> future = futureMap.remove(requestId);
        if (future != null) {
            future.complete(response);
        }
    }
}