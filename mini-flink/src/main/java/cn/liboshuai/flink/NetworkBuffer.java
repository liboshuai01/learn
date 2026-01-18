package cn.liboshuai.flink;

import java.nio.charset.StandardCharsets;

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
