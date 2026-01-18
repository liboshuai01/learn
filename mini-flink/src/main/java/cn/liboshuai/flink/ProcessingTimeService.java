package cn.liboshuai.flink;

import java.util.concurrent.ScheduledFuture;

/**
 * 对应 Flink 的 ProcessingTimeService。
 * 提供对处理时间（Processing Time）的访问和定时器注册服务。
 */
public interface ProcessingTimeService {

    /**
     * 获取当前的处理时间（通常是系统时间）。
     */
    long getCurrentProcessingTime();

    /**
     * 注册一个定时器，在指定的时间戳触发回调。
     *
     * @param timestamp 触发时间
     * @param callback  回调逻辑
     * @return 用于取消定时器的 Future
     */
    ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback);

    /**
     * 关闭服务，释放资源（如线程池）。
     */
    void shutdownService();
}
