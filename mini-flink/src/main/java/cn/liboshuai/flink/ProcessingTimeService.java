package cn.liboshuai.flink;

import java.util.concurrent.ScheduledFuture;

public interface ProcessingTimeService {

    long getCurrentProcessingTime();

    ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback);

    void shutdownService();
}
