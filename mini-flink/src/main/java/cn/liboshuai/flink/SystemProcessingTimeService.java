package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SystemProcessingTimeService implements ProcessingTimeService{

    private final ScheduledExecutorService timerService;

    public SystemProcessingTimeService() {
        // 创建一个核心线程数为 1 的调度线程池，类似 Flink 的默认行为
        this.timerService = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "Flink-System-Timer-Service");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public long getCurrentProcessingTime() {
        return System.currentTimeMillis();
    }

    @Override
    public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
        long delay = Math.max(0, timestamp - getCurrentProcessingTime());
        return timerService.schedule(() -> {
            try {
                callback.onProcessingTime(timestamp);
            } catch (Exception e) {
                log.error("定时器回调执行异常", e);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdownService() {
        if (!timerService.isShutdown()) {
            timerService.shutdownNow();
        }
    }
}
