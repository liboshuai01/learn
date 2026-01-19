package cn.liboshuai.demo.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * 5. (已修改) 模拟一个服务，演示如何“抛出”和“处理”我们定义的异常
 */
public class MyServiceSimulator {

    private static final Logger log = LoggerFactory.getLogger(MyServiceSimulator.class);

    private boolean initialized = false;
    private final Properties config = new Properties();

    /**
     * 新增：服务实例的ID，模拟 JobID
     */
    private final String serviceId;

    /**
     * 新增：构造函数，用于设置服务ID
     */
    public MyServiceSimulator(String serviceId) {
        this.serviceId = serviceId;
    }

    /**
     * 演示：抛出“受检”异常 (此方法未修改)
     * (抛出 InvalidConfigurationException)
     */
    public void loadConfig(String configPath) throws InvalidConfigurationException {
        if (configPath == null || configPath.isEmpty()) {
            throw new InvalidConfigurationException("Config path cannot be null or empty.");
        }

        try (FileInputStream fis = new FileInputStream(configPath)) {
            config.load(fis);

            if (config.getProperty("server.port") == null) {
                throw new InvalidConfigurationException("Configuration missing required key: 'server.port'");
            }

            Integer.parseInt(config.getProperty("server.port"));

            this.initialized = true;

        } catch (FileNotFoundException e) {
            throw new InvalidConfigurationException("Config file not found at: " + configPath, e);
        } catch (NumberFormatException e) {
            throw new InvalidConfigurationException("Invalid port number: " + config.getProperty("server.port"), e);
        } catch (IOException e) {
            throw new InvalidConfigurationException("Failed to read config file: " + configPath, e);
        }
    }

    /**
     * 演示：抛出“非受检”异常 (已修改)
     * <p>
     * 1. 保留了原有的 InvalidOperationException (不带ID)
     * 2. 新增了抛出 ServiceExecutionException (带ID)
     */
    public void run() {
        if (!initialized) {
            // 1. 抛出原有的异常：表示程序状态不正确（编程错误）
            // 注意：这个异常类没有 serviceId 属性
            throw new InvalidOperationException("Service is not initialized. Cannot run(). Did you forget to call loadConfig()?");
        }

        log.info("Service [{}] running with port: {}", this.serviceId, config.getProperty("server.port"));

        // 2. 模拟抛出新的、带上下文的异常
        // 假设端口 9999 是一个非法的运行时端口
        if ("9999".equals(config.getProperty("server.port"))) {
            // 关键：抛出我们新增的 ServiceExecutionException，
            // 并将服务自己的 ID (this.serviceId) 传给它。
            throw new ServiceExecutionException(
                    this.serviceId,
                    "Simulated critical failure: Port 9999 is reserved for failures.",
                    null);
        }

        log.info("Service [{}] completed run successfully.", this.serviceId);
    }

    /**
     * 新增一个辅助方法，用于在 main 中模拟配置加载成功
     */
    public void simulateSuccessfulLoad(String port) {
        this.config.setProperty("server.port", port);
        this.initialized = true;
        log.info("Service [{}] configuration successfully simulated with port {}.", this.serviceId, port);
    }


    /**
     * 6. Main 方法 (已修改，增加场景3)
     */
    public static void main(String[] args) {
        // --- 场景 1: 处理“受检”异常 (和原来一样) ---
        log.info("--- 尝试加载配置 (场景 1: 捕获受检异常 - 失败路径) ---");
        MyServiceSimulator service1 = new MyServiceSimulator("Service-001");
        try {
            service1.loadConfig("nonexistent-file.properties");
            service1.run();
        } catch (InvalidConfigurationException e) {
            // 旧异常，没有 getServiceId() 方法
            log.error("配置失败 (Specific Catch): ", e);
        }

        log.info("--- 尝试运行服务 (场景 2: 捕获非受检异常 - 状态错误) ---");
        // --- 场景 2: 处理“非受检”异常 (和原来一样) ---
        // 注意：因为场景 1 失败了，service1.initialized 仍然是 false
        try {
            service1.run();
        } catch (InvalidOperationException e) {
            // 旧异常，没有 getServiceId() 方法
            log.error("操作失败 (Unchecked Catch): ", e);
        }

        // --- 场景 3: 新增 - 捕获我们新增的、带ID的异常 ---
        log.info("--- 尝试运行服务 (场景 3: 捕获带ID的新异常) ---");
        MyServiceSimulator service2 = new MyServiceSimulator("FailureTestService-002");
        try {
            // 我们用辅助方法模拟配置加载成功，并设置一个会触发错误的端口
            service2.simulateSuccessfulLoad("9999");

            // 这行代码将抛出 ServiceExecutionException
            service2.run();

        } catch (ServiceExecutionException e) {
            // 关键点：
            // 我们捕获了新定义的异常，并可以调用 e.getServiceId()
            // 来获取是哪个服务实例出了问题！
            log.error("服务执行失败 (Service ID: {}): {}", e.getServiceId(), e.getMessage());

        } catch (InvalidOperationException e) {
            // 兜底捕获（虽然在这个场景中不会触发）
            log.error("操作失败 (Unchecked Catch): ", e);
        }
    }
}