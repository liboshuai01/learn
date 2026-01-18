package cn.liboshuai.flink;

/**
 * 对应 Flink 的 ProcessingTimeCallback。
 * 当定时器触发时调用的回调接口。
 */
@FunctionalInterface
public interface ProcessingTimeCallback {
    /**
     * 当处理时间到达预定时间戳时调用。
     *
     * @param timestamp 触发的时间戳
     */
    void onProcessingTime(long timestamp) throws Exception;
}
