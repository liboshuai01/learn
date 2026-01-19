package cn.liboshuai.demo.exception;


/**
 * 2. 一个具体的“受检”异常
 * 继承自我们的基类 MyProjectException。
 * 用于表示配置错误的特定情况。
 */
public class InvalidConfigurationException extends MyProjectException {

    private static final long serialVersionUID = 1L;

    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * 这是最重要的构造函数。
     * 当我们捕获一个底层异常（比如 IOException）并将其“包装”为
     * InvalidConfigurationException 时，我们会使用这个。
     *
     * @param message 描述性的错误信息
     * @param cause   原始的异常（例如 IOException, NumberFormatException 等）
     */
    public InvalidConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
