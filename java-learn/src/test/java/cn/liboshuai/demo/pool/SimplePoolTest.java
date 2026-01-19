package cn.liboshuai.demo.pool;


import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SimplePoolTest {

    @Test
    public void testConcurrentGetConnection() throws InterruptedException {
        // 1. 配置连接池：最大5个连接，超时1秒
        SimplePoolConfig config = SimplePoolConfig.builder()
                .driverClassName("com.mysql.cj.jdbc.Driver")
                .url("jdbc:mysql://test:3306/test")
                .username("root")
                .password("9ol.(OL>")
                .coreSize(2)
                .maxSize(5)
                .timeoutMs(1000)
                .build();

        MiniHikariPool pool = new MiniHikariPool(config);

        // 2. 模拟 20 个线程并发抢夺 5 个连接
        int threadCount = 20;
        CountDownLatch startSignal = new CountDownLatch(1); // 发令枪
        CountDownLatch doneSignal = new CountDownLatch(threadCount); // 结束信号
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startSignal.await(); // 等待发令枪响

                    // 尝试获取连接
                    log.info(Thread.currentThread().getName() + " 尝试获取连接...");
                    try (Connection conn = pool.getConnection()) {
                        log.info(Thread.currentThread().getName() + " 获取成功! " + conn);
                        successCount.incrementAndGet();
                        // 模拟业务耗时 100ms
                        Thread.sleep(100);
                    } // try-with-resources 会自动调用 close()，即触发 recycle()

                } catch (SQLException e) {
                    log.warn(Thread.currentThread().getName() + " 获取失败: " + e.getMessage());
                    failCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneSignal.countDown();
                }
            });
        }

        // 3. 开始并发测试
        log.info("----- 测试开始 -----");
        startSignal.countDown(); // 发令
        doneSignal.await(); // 等待所有线程结束
        executor.shutdown();

        log.info("----- 测试结束 -----");
        log.info("成功次数: {}", successCount.get());
        log.info("超时/失败次数: {}", failCount.get());

        // 验证逻辑：如果配置合理，应该会有部分成功，部分因为排队太久而超时
    }
}