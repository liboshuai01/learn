package cn.liboshuai.demo.function;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 模拟 Flink 的 FunctionUtils 工具类 (模块三)
 *
 * 核心功能：提供 "unchecked" 桥梁方法。
 * 它接收我们自定义的 "WithException" 接口，
 * 并返回一个标准的 "java.util.function" 接口。
 */
public class FunctionUtils {

    /**
     * “解包” Supplier
     * 将 Flink 的 SupplierWithException 转换为标准的 Java 8 Supplier。
     *
     * @param supplierWithException Flink 风格的 supplier
     * @return Java 8 风格的 supplier
     */
    public static <T> Supplier<T> uncheckedSupplier(
            SupplierWithException<T, ?> supplierWithException) {

        // 返回一个标准的 Java 8 Lambda
        return () -> {
            try {
                // 1. 在内部调用 Flink 接口
                return supplierWithException.get();
            } catch (Throwable t) {
                // 2. 捕获异常并“解包” (rethrow)
                ExceptionUtils.rethrow(t);
                return null; // 编译器需要，实际上走不到这里
            }
        };
    }

    /**
     * “解包” Function
     *
     * @param functionWithException Flink 风格的 function
     * @return Java 8 风格的 function
     */
    public static <A, B> Function<A, B> uncheckedFunction(
            FunctionWithException<A, B, ?> functionWithException) {

        return (A value) -> {
            try {
                // 1. 调用 Flink 接口
                return functionWithException.apply(value);
            } catch (Throwable t) {
                // 2. 捕获并“解包”
                ExceptionUtils.rethrow(t);
                return null;
            }
        };
    }

    /**
     * “解包” Consumer
     *
     * @param throwingConsumer Flink 风格的 consumer
     * @return Java 8 风格的 consumer
     */
    public static <T> Consumer<T> uncheckedConsumer(
            ThrowingConsumer<T, ?> throwingConsumer) {

        return (T value) -> {
            try {
                // 1. 调用 Flink 接口
                throwingConsumer.accept(value);
            } catch (Throwable t) {
                // 2. 捕获并“解包”
                ExceptionUtils.rethrow(t);
            }
        };
    }
}