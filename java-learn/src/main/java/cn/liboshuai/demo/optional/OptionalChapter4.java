package cn.liboshuai.demo.optional;

import java.util.Optional;
import java.util.function.Supplier;

public class OptionalChapter4 {

    /**
     * 这是一个辅助方法，用来模拟一个“昂贵”的操作，比如数据库查询或复杂的计算。
     * 每次调用它时，它都会打印日志，以便我们跟踪它是否被执行。
     * @return 一个默认字符串
     */
    private static String getExpensiveDefaultValue() {
        System.out.println("  -> (日志：正在执行 getExpensiveDefaultValue()... 这可能很耗时...)");
        return "Default-Value";
    }

    /**
     * 这是一个辅助方法，用于模拟一个自定义异常的 Supplier
     * @return 一个 Supplier
     */
    private static Supplier<IllegalStateException> getCustomExceptionSupplier() {
        return () -> new IllegalStateException("关键值未在配置中提供！");
    }

    public static void main(String[] args) {

        Optional<String> presentOptional = Optional.of("Flink-Config-Value");
        Optional<String> emptyOptional = Optional.empty();

        System.out.println("--- 1. orElse(T other) ---");
        // orElse(T other): 如果为空，返回 T

        // 场景 A：Optional 为空，orElse 生效
        String value1 = emptyOptional.orElse("Simple-Default");
        System.out.println("emptyOptional.orElse(): " + value1); // 输出: Simple-Default

        // 场景 B：Optional 有值，orElse 不生效（但仍有陷阱）
        String value2 = presentOptional.orElse("Simple-Default");
        System.out.println("presentOptional.orElse(): " + value2); // 输出: Flink-Config-Value


        System.out.println("\n--- 2. orElse(T other) 的性能陷阱 ---");
        // 演示 orElse() 总是会执行参数中的方法

        System.out.println("测试 presentOptional (有值) + orElse():");
        // 即使 presentOptional 有值，getExpensiveDefaultValue() 仍然被执行了！
        String value3 = presentOptional.orElse(getExpensiveDefaultValue());
        System.out.println("  -> 最终结果: " + value3); // Flink-Config-Value

        System.out.println("\n--- 3. orElseGet(Supplier<? extends T> other) ---");
        // orElseGet(Supplier): 如果为空，才执行 Supplier

        System.out.println("测试 presentOptional (有值) + orElseGet():");
        // presentOptional 有值，Lambda (Supplier) 不会执行
        String value4 = presentOptional.orElseGet(() -> getExpensiveDefaultValue());
        System.out.println("  -> (日志：没有打印，说明 getExpensiveDefaultValue() 未被调用)");
        System.out.println("  -> 最终结果: " + value4); // Flink-Config-Value

        System.out.println("\n测试 emptyOptional (为空) + orElseGet():");
        // emptyOptional 为空，Lambda (Supplier) 被执行
        String value5 = emptyOptional.orElseGet(() -> getExpensiveDefaultValue());
        System.out.println("  -> 最终结果: " + value5); // Default-Value

        System.out.println("\n--- 4. orElseThrow(Supplier<? extends X> exceptionSupplier) ---");
        // orElseThrow(Supplier): 如果为空，抛出 Supplier 提供的异常

        // 场景 A：有值的情况 (安全)
        try {
            String value6 = presentOptional.orElseThrow(getCustomExceptionSupplier());
            System.out.println("presentOptional.orElseThrow(): " + value6); // Flink-Config-Value
        } catch (IllegalStateException e) {
            System.out.println("  -> (这不应该发生) " + e.getMessage());
        }

        // 场景 B：为空的情况 (抛出异常)
        try {
            System.out.println("测试 emptyOptional.orElseThrow()...");
            String value7 = emptyOptional.orElseThrow(getCustomExceptionSupplier());
            System.out.println("  -> (这行不会打印) 值为: " + value7);
        } catch (IllegalStateException e) {
            System.out.println("  -> 成功捕获异常: " + e.getClass().getName());
            System.out.println("  -> 异常信息: " + e.getMessage());
        }
    }
}
