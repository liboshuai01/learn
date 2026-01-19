package cn.liboshuai.demo.pool;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SimplePoolConfig {
    private String url;
    private String username;
    private String password;
    private String driverClassName;

    // 核心参数：最小空闲、最大连接数、超时时间
    private int coreSize = 5;
    private int maxSize = 10;
    private long timeoutMs = 3000;
}