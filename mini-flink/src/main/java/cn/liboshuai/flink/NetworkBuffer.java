package cn.liboshuai.flink;

import java.nio.charset.StandardCharsets;

/**
 * 模拟 Flink 的网络 Buffer (org.apache.flink.runtime.io.network.buffer.Buffer)。
 * 在真实 Flink 中，这里封装的是 MemorySegment (堆外内存/堆内存)。
 * 这里为了简化，我们内部持有一个字节数组来模拟数据。
 */
public class NetworkBuffer {

    private final byte[] data;

    public NetworkBuffer(String content) {
        this.data = content.getBytes(StandardCharsets.UTF_8);
    }

    public NetworkBuffer(byte[] data) {
        this.data = data;
    }

    public int getSize() {
        return data.length;
    }

    public byte[] getBytes() {
        return data;
    }

    public String toStringContent() {
        return new String(data, StandardCharsets.UTF_8);
    }
}