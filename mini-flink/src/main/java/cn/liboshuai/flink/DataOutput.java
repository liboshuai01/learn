package cn.liboshuai.flink;

public interface DataOutput {
    void processRecord(String record);
}
