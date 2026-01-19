package cn.liboshuai.demo.exception;

/**
 * 新增的异常类：服务执行异常
 * <p>
 * 类似于 Flink 的 JobExecutionException，这个异常类持有一个
 * 特定的上下文属性 (serviceId)，以便调用者可以识别是哪个服务
 * 实例在执行时出了问题。
 * <p>
 * 我们让它继承自 MyProjectRuntimeException，使其成为一个“非受检”异常。
 */
public class ServiceExecutionException extends MyProjectRuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 关键属性：类似于 JobID，用于标识出问题的服务实例
     */
    private final String serviceId;

    public ServiceExecutionException(String serviceId, String message) {
        // 调用父类 (MyProjectRuntimeException) 的构造函数
        super(message);
        this.serviceId = serviceId;
    }

    public ServiceExecutionException(String serviceId, String message, Throwable cause) {
        // 调用父类 (MyProjectRuntimeException) 的构造函数
        super(message, cause);
        this.serviceId = serviceId;
    }

    /**
     * 公共方法：供 catch 块的调用者获取是哪个服务出了问题
     *
     * @return 服务ID
     */
    public String getServiceId() {
        return serviceId;
    }
}
