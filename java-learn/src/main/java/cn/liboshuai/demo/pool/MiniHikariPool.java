package cn.liboshuai.demo.pool;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 优化后的简易连接池
 */
@Slf4j
public class MiniHikariPool {

    private final SimplePoolConfig config;
    // 修改点1: 队列存储 PoolEntry 包装对象，携带时间戳信息
    private final BlockingQueue<PoolEntry> idleQueue;
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private volatile boolean isShutdown = false;

    // 优化点: 500ms 内的连接免检，大幅提升性能
    private static final long VALIDATION_TIMEOUT_WINDOW_MS = 500L;

    public MiniHikariPool(SimplePoolConfig config) {
        this.config = config;
        this.idleQueue = new ArrayBlockingQueue<>(config.getMaxSize());
        init();
    }

    private void init() {
        try {
            Class.forName(config.getDriverClassName());
            for (int i = 0; i < config.getCoreSize(); i++) {
                Connection conn = createRealConnection();
                if (conn != null) {
                    PoolEntry entry = new PoolEntry(conn, System.currentTimeMillis());
                    if (idleQueue.offer(entry)) {
                        totalConnections.incrementAndGet();
                    } else {
                        closeRealConnection(conn);
                    }
                }
            }
            log.info("连接池初始化完成，当前连接数: {}", totalConnections.get());
        } catch (Exception e) {
            throw new RuntimeException("连接池初始化失败", e);
        }
    }

    /**
     * 关闭连接池（新增方法）
     */
    public void shutdown() {
        if (isShutdown) return;
        isShutdown = true;
        log.info("正在关闭连接池...");

        // 1. 销毁空闲队列中的连接
        PoolEntry entry;
        while ((entry = idleQueue.poll()) != null) {
            closeRealConnection(entry.getConnection());
        }

        // 2. 这里的 totalConnections 可能不为0，因为还有正在被借出的连接
        // 实际生产中可能需要等待它们归还，这里简化处理。
        log.info("连接池已关闭");
    }

    public Connection getConnection() throws SQLException {
        if (isShutdown) throw new SQLException("连接池已关闭");

        long startNanos = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(config.getTimeoutMs());

        while (true) {
            long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
            if (remainingNanos <= 0) {
                throw new SQLException("获取连接超时，等待时间: " + config.getTimeoutMs() + "ms");
            }

            PoolEntry entry = null;
            try {
                // 1. 尝试直接获取
                entry = idleQueue.poll();

                if (entry == null) {
                    // 2. 队列为空，尝试创建
                    if (tryCreateNewConnection()) {
                        // 创建物理连接
                        Connection realConn = createRealConnection();
                        entry = new PoolEntry(realConn, System.currentTimeMillis());
                    } else {
                        // 3. 无法创建，阻塞等待
                        entry = idleQueue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("获取连接被中断");
            } catch (SQLException e) {
                totalConnections.decrementAndGet();
                log.warn("创建物理连接失败", e);
                continue;
            }

            // 4. 有效性检查 (含性能优化)
            if (entry != null) {
                Connection realConn = entry.getConnection();
                long now = System.currentTimeMillis();

                // 优化点: 如果连接最近刚被使用过(500ms内)，则跳过 isValid 检查
                // 这种策略叫 "Elide Valid Check"
                if ((now - entry.getLastAccessedTime() < VALIDATION_TIMEOUT_WINDOW_MS) || isValid(realConn)) {
                    // 包装并返回
                    return wrapConnection(realConn);
                } else {
                    // 连接失效，销毁并重试
                    log.debug("发现失效连接，已丢弃，重试获取...");
                    closeRealConnection(realConn);
                    totalConnections.decrementAndGet();
                    // loop continue
                }
            }
        }
    }

    private boolean tryCreateNewConnection() {
        while (true) {
            int current = totalConnections.get();
            if (current >= config.getMaxSize()) {
                return false;
            }
            if (totalConnections.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private Connection wrapConnection(Connection realConn) {
        return (Connection) Proxy.newProxyInstance(
                MiniHikariPool.class.getClassLoader(),
                new Class[]{Connection.class},
                new ConnectionHandler(realConn, this)
        );
    }

    // 将方法改为 public 或 default 供 Handler 调用
    void recycle(Connection realConn) {
        if (isShutdown) {
            closeRealConnection(realConn);
            totalConnections.decrementAndGet();
            return;
        }

        // 简单检查连接状态
        if (isRealClosed(realConn)) {
            totalConnections.decrementAndGet();
            return; // 已经关闭的连接不入队
        }

        // 封装为 PoolEntry，更新时间戳
        PoolEntry entry = new PoolEntry(realConn, System.currentTimeMillis());

        if (!idleQueue.offer(entry)) {
            log.warn("归还连接失败(队列满)，可能存在连接泄漏或逻辑错误，销毁连接");
            closeRealConnection(realConn);
            totalConnections.decrementAndGet();
        }
    }

    private boolean isRealClosed(Connection conn) {
        try {
            return conn == null || conn.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    private boolean isValid(Connection conn) {
        try {
            if (conn.isClosed()) return false;
            return conn.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }

    private Connection createRealConnection() throws SQLException {
        return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
    }

    private void closeRealConnection(Connection conn) {
        try {
            if (conn != null) conn.close();
        } catch (SQLException ignored) {}
    }

    // 内部类：包装连接和时间戳
    @Data
    @AllArgsConstructor
    static class PoolEntry {
        private Connection connection;
        private long lastAccessedTime;
    }

    // 内部类：代理处理器
    static class ConnectionHandler implements InvocationHandler {
        private final Connection realConn;
        private final MiniHikariPool pool;
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        public ConnectionHandler(Connection realConn, MiniHikariPool pool) {
            this.realConn = realConn;
            this.pool = pool;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            // 1. 拦截 close 方法，执行归还逻辑
            if ("close".equals(methodName)) {
                if (isClosed.compareAndSet(false, true)) {
                    pool.recycle(realConn);
                }
                return null;
            }

            // 2. 拦截 isClosed 方法
            if ("isClosed".equals(methodName)) {
                return isClosed.get();
            }

            // 3. (修复点) 拦截 toString，避免打印出代理对象的乱码，方便调试
            if ("toString".equals(methodName)) {
                return "MiniHikariPool-Proxy-Connection-" + System.identityHashCode(proxy);
            }

            // 4. (修复点) 拦截 equals/hashCode，避免将代理放入集合时出现问题
            if ("equals".equals(methodName)) {
                return proxy == args[0];
            }
            if ("hashCode".equals(methodName)) {
                return System.identityHashCode(proxy);
            }

            // 5. 状态检查：如果逻辑上已关闭，禁止调用其他方法
            if (isClosed.get()) {
                throw new SQLException("Connection has been closed");
            }

            return method.invoke(realConn, args);
        }
    }
}