package cn.liboshuai.demo.exception;


/**
 * 4. 一个具体的“非受检”异常
 * 类似于 Flink 的 RpcInvalidStateException。
 * 例如，当服务还未初始化时，调用者就尝试调用 run() 方法。
 */
public class InvalidOperationException extends MyProjectRuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidOperationException(String message) {
        super(message);
    }

    public InvalidOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
