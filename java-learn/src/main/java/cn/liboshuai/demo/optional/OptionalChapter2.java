package cn.liboshuai.demo.optional;

import java.util.Optional;

public class OptionalChapter2 {

    public static void main(String[] args) {
        System.out.println("--- 1. Optional.empty() ---");
        // 1. Optional.empty(): 创建一个“空”的 Optional
        // 它总是返回一个内部没有值的 Optional 实例
        Optional<String> emptyOptional = Optional.empty();
        System.out.println("emptyOptional: " + emptyOptional);
        System.out.println("emptyOptional 是否存在值? " + emptyOptional.isPresent()); // false

        System.out.println("\n--- 2. Optional.of(value) ---");
        // 2. Optional.of(value): 创建一个“确定存在”的 Optional
        // value 必须是非 null 的
        String name = "Flink 1.18";
        Optional<String> nameOptional = Optional.of(name);
        System.out.println("nameOptional: " + nameOptional);
        System.out.println("nameOptional 是否存在值? " + nameOptional.isPresent()); // true

        // 演示 Optional.of() 传入 null 时的情况
        try {
            String nullName = null;
            Optional<String> errorOptional = Optional.of(nullName);
            System.out.println("这行不会被打印");
        } catch (NullPointerException e) {
            System.out.println("Optional.of(null) 捕获到异常: " + e.getClass().getName());
        }

        System.out.println("\n--- 3. Optional.ofNullable(value) ---");
        // 3. Optional.ofNullable(value): 创建一个“可能为空”的 Optional
        // 这是最安全、最常用的方法

        // 场景 A：传入一个非 null 值
        String jobName = "DataStream Job";
        Optional<String> jobOptional = Optional.ofNullable(jobName);
        System.out.println("jobOptional (非 null 值): " + jobOptional);
        System.out.println("jobOptional 是否存在值? " + jobOptional.isPresent()); // true

        // 场景 B：传入一个 null 值
        String configValue = null;
        Optional<String> configOptional = Optional.ofNullable(configValue);
        System.out.println("configOptional (null 值): " + configOptional);
        System.out.println("configOptional 是否存在值? " + configOptional.isPresent()); // false
    }
}
