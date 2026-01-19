package cn.liboshuai.demo.function;

/**
 * 模拟 Flink 的 ExceptionUtils 工具类 (模块三)
 * 核心功能：rethrow(t)
 * 它可以将一个受检异常“伪装”成 RuntimeException 抛出，
 * 从而“欺骗”编译器，绕过 Java 8 标准函数式接口的 throws 限制。
 */
public class ExceptionUtils {

    /**
     * 重新抛出异常，如果是受检异常，则包装为 RuntimeException。
     * Flink 源码中的实现更复杂，它会尝试直接抛出受检异常而不包装，
     * 这里我们使用一个简化的、但效果一致的实现。
     *
     * @param t 捕获到的异常
     */
    public static void rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            // 如果已经是 RuntimeException，直接抛出
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            // 如果是 Error，也直接抛出
            throw (Error) t;
        }
        // 如果是受检异常 (Checked Exception)，包装成 RuntimeException 抛出
        // 这就是“解包”的核心：把 Checked 转换成 Unchecked
        throw new RuntimeException("Checked exception caught and rethrown", t);
    }

    /**
     * Flink 中更“激进”的 rethrow 版本，利用泛型擦除
     * 来直接抛出受检异常，连包装都不需要。
     *
     * @param throwable 异常
     * @param <T>       异常类型
     * @throws T 抛出的异常
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrowJvm(Throwable throwable) throws T {
        // 这个 <T> 泛型会在编译时“欺骗”编译器，
        // 让它以为调用者 "throws T" 了，但运行时 T 被擦除，
        // JVM 会直接抛出原始的 throwable。
        throw (T) throwable;
    }
}
