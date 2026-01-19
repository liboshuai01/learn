package cn.liboshuai.demo.function;

/**
 * 模拟 Flink 的 ThrowingConsumer 接口 (模块二)
 *
 * 这是一个函数式接口，其 accept 方法可以抛出受检异常 E。
 *
 * @param <T> 输入类型
 * @param <E> 异常类型
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Throwable> {

    /**
     * 消费一个值
     *
     * @param value 输入值
     * @throws E 可能抛出的异常
     */
    void accept(T value) throws E;
}
