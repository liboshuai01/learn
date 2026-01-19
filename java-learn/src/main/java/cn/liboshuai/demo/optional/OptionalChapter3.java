package cn.liboshuai.demo.optional;

import java.util.Optional;
import java.util.NoSuchElementException;

public class OptionalChapter3 {

    public static void main(String[] args) {

        // 准备两个 Optional 实例，一个有值，一个为空
        Optional<String> presentOptional = Optional.of("Flink 1.18");
        Optional<String> emptyOptional = Optional.empty();

        System.out.println("--- 1. isPresent(): 检查值是否存在 ---");
        System.out.println("presentOptional.isPresent(): " + presentOptional.isPresent()); // true
        System.out.println("emptyOptional.isPresent(): " + emptyOptional.isPresent());   // false

        System.out.println("\n--- 2. ifPresent(Consumer): 安全地执行操作 ---");
        // ifPresent 接受一个 Lambda 表达式
        // 只有当 Optional 有值时，Lambda 才会被执行

        // 场景 A：有值的情况
        System.out.println("执行 presentOptional.ifPresent():");
        presentOptional.ifPresent(value -> {
            // 这段代码会被执行
            System.out.println("  -> 值存在, 值为: " + value.toUpperCase());
        });

        // 场景 B：为空的情况
        System.out.println("执行 emptyOptional.ifPresent():");
        emptyOptional.ifPresent(value -> {
            // 这段代码不会被执行
            System.out.println("  -> (这行不会打印) 值存在: " + value);
        });
        System.out.println("  -> (ifPresent 结束)");


        System.out.println("\n--- 3. get(): 不安全地获取值 ---");
        // get() 在有值时返回值，在没值时抛出异常

        // 场景 A：有值的情况 (安全)
        if (presentOptional.isPresent()) {
            String value = presentOptional.get();
            System.out.println("presentOptional.get(): " + value);
        }

        // 场景 B：为空的情况 (危险!)
        try {
            System.out.println("尝试调用 emptyOptional.get()...");
            String value = emptyOptional.get(); // 这将抛出异常
            System.out.println("  -> (这行不会打印) 值为: " + value);
        } catch (NoSuchElementException e) {
            System.out.println("  -> 捕获到异常: " + e.getClass().getName());
        }


        System.out.println("\n--- 4. 反模式: isPresent() + get() ---");
        // 虽然安全，但不推荐，因为代码很啰嗦
        String greeting;
        if (presentOptional.isPresent()) {
            greeting = "Hello, " + presentOptional.get();
        } else {
            greeting = "Hello, Guest";
        }
        System.out.println("反模式写法: " + greeting);

        // 更好的写法（将在第四章、第五章学到）
        // 预览：
        // String betterGreeting = presentOptional.map(v -> "Hello, " + v).orElse("Hello, Guest");
        // System.out.println("推荐写法: " + betterGreeting);
    }
}
