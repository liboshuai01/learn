package cn.liboshuai.flink;

@FunctionalInterface
public interface ProcessingTimeCallback {
    void onProcessingTime(long timestamp) throws  Exception;
}
