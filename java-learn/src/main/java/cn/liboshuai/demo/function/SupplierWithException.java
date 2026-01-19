package cn.liboshuai.demo.function;

/**
 * 模拟 Flink 的 SupplierWithException 接口 (模块二)
 * 这是一个函数式接口，其 get 方法可以抛出受检异常 E。
 *
 * @param <R> 返回类型
 * @param <E> 异常类型
 */
@FunctionalInterface
public interface SupplierWithException<R, E extends Throwable> {

    /**
     * 获取一个结果
     *
     * @return 结果
     * @throws E 可能抛出的异常
     */
    R get() throws E;
}
