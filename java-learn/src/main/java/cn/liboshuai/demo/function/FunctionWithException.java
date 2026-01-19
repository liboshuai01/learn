package cn.liboshuai.demo.function;

/**
 * 模拟 Flink 的 FunctionWithException 接口 (模块二)
 * 这是一个函数式接口，其 apply 方法可以抛出受检异常 E。
 * @param <T> 输入类型
 * @param <R> 返回类型
 * @param <E> 异常类型
 */
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Throwable> {

    /**
     * 应用函数
     *
     * @param value 输入值
     * @return 函数结果
     * @throws E 可能抛出的异常
     */
    R apply(T value) throws E;
}
