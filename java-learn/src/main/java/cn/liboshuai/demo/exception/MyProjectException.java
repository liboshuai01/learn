package cn.liboshuai.demo.exception;

/**
 * 1. 自定义“受检”异常的基类
 * 类似于 Flink 的 FlinkException，用于所有我们项目中定义的、
 * 希望调用者必须处理（try-catch 或 throws）的异常。
 */
public class MyProjectException extends Exception {

    private static final long serialVersionUID = 1L;

    public MyProjectException(String message) {
        super(message);
    }

    public MyProjectException(String message, Throwable cause) {
        super(message, cause);
    }
}