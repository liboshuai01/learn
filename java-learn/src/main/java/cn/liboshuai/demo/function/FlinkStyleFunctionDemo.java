package cn.liboshuai.demo.function;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Java 8 与 Flink 函数式编程实战 Demo
 *
 * 运行 main 方法，按顺序查看三个模块的演示。
 */
public class FlinkStyleFunctionDemo {

    public static void main(String[] args) {
        // 模块一：Java 8 核心函数式编程
        demoModule1_Java8Core();

        // 模块二：Flink "魔改"的 ...WithException 接口
        demoModule2_FlinkInterfaces();

        // 模块三：“受检”与“非受检”的桥梁 (unchecked)
        demoModule3_Bridge();
    }

    // =========================================================================
    // 模块一：Java 8 核心
    // =========================================================================
    private static void demoModule1_Java8Core() {
        System.out.println("--- 模块一：Java 8 核心函数式编程 ---");

        // 1. Runnable (无参，无返回)
        // 传统方式
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("[M1] Java 7 匿名内部类");
            }
        }).start();

        // Lambda 方式
        new Thread(() -> System.out.println("[M1] Java 8 Lambda")).start();

        // 2. 四大金刚 (Function, Supplier, Consumer, Predicate)
        List<String> data = Arrays.asList("Flink", "Spark", "", "Java 8");

        // Function<T, R> (转换)：s -> s.length()
        // 使用方法引用 (任意对象的实例方法)：String::length
        System.out.println("[M1] Function 示例:");
        data.stream()
                .map(String::length) // 等价于 s -> s.length()
                .forEach(len -> System.out.print(len + " ")); // 5 5 0 6
        System.out.println();

        // Predicate<T> (判断)：s -> !s.isEmpty()
        // 使用方法引用：String::isEmpty，然后再 negate
        System.out.println("[M1] Predicate 示例:");
        long count = data.stream()
                .filter(((Predicate<String>) String::isEmpty).negate()) // 等价于 s -> !s.isEmpty()
                .count();
        System.out.println("非空元素个数: " + count); // 3

        // Consumer<T> (消费)：s -> System.out.println(s)
        // 使用方法引用 (特定对象的实例方法)：System.out::println
        System.out.println("[M1] Consumer 示例:");
        data.stream().forEach(System.out::println);

        // Supplier<T> (供给)：() -> new String("Hello")
        // 使用方法引用 (构造函数引用)：String::new
        Supplier<String> supplier = String::new;
        System.out.println("[M1] Supplier 示例 (为空): " + supplier.get());

        // 3. 闭包 (Effectively Final)
        String prefix = "User: ";
        // prefix = "Admin: "; // 如果取消这行注释，下面的 Lambda 会编译失败
        Consumer<String> printer = (name) -> System.out.println("[M1] 闭包示例: " + prefix + name);
        printer.accept("Alice");

        // 等待线程执行完毕
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    // =========================================================================
    // 模块二：Flink 的 "WithException" 接口
    // =========================================================================

    /**
     * 这是一个模拟的 I/O 操作，它会抛出受检异常 IOException
     */
    private static String riskyOperation(boolean shouldFail) throws IOException {
        if (shouldFail) {
            // 模拟文件不存在
            throw new IOException("文件读取失败! (模拟的受检异常)");
        }
        return "从 Kafka 读取到的数据";
    }

    private static void demoModule2_FlinkInterfaces() {
        System.out.println("\n--- 模块二：Flink 的 ...WithException 接口 ---");

        // 1. JDK 标准接口的“痛点”
        // 我们想把 riskyOperation 传给一个 Supplier，但不行：
        // Supplier<String> standardSupplier = () -> riskyOperation(true); // 编译失败!
        // Unhandled exception: java.io.IOException

        // 唯一的办法是在 Lambda 内部“吞掉”异常
        Supplier<String> uglySupplier = () -> {
            try {
                return riskyOperation(true);
            } catch (IOException e) {
                // 没办法，只能转成 RuntimeException
                throw new RuntimeException("[M2] 丑陋的 try-catch 包装", e);
            }
        };

        // 2. Flink 的优雅方案
        // Flink 自定义的接口允许 Lambda 抛出异常
        SupplierWithException<String, IOException> flinkSupplier =
                () -> riskyOperation(true); // 非常干净！

        // Flink 风格的 Lambda
        FunctionWithException<String, Long, IOException> flinkFunction =
                (path) -> Files.size(Paths.get(path)); // 干净！

        // 异常在 *调用方* 处理，而不是在 Lambda 内部
        System.out.println("[M2] Flink 接口演示 (调用会失败):");
        try {
            String result = flinkSupplier.get(); // 调用方负责 try-catch
            System.out.println("成功: " + result);
        } catch (IOException e) {
            System.out.println("捕获到了受检异常: " + e.getMessage());
        }

        // 演示成功的情况
        SupplierWithException<String, IOException> flinkSupplierSuccess =
                () -> riskyOperation(false); // 不会失败
        try {
            System.out.println("[M2] Flink 接口演示 (调用会成功):");
            String result = flinkSupplierSuccess.get();
            System.out.println("成功: " + result);
        } catch (IOException e) {
            // 这次不会执行
            System.out.println("捕获到了受检异常: " + e.getMessage());
        }
    }


    // =========================================================================
    // 模块三：连接 Flink 与 Java 8 的桥梁
    // =========================================================================
    private static void demoModule3_Bridge() {
        System.out.println("\n--- 模块三：'unchecked' 桥梁 ---");

        // 场景：我们有一个 Flink 风格的 Lambda...
        SupplierWithException<String, IOException> flinkSupplier =
                () -> riskyOperation(true); // 会抛出 IOException

        // ...但我们必须把它传给一个只接受 Java 8 标准接口的 API
        // 比如 CompletableFuture.supplyAsync(Supplier<T> supplier)

        // 1. “解包”：使用 FunctionUtils.uncheckedSupplier
        // 它会返回一个标准的 Supplier<String>，
        // 它在内部会 rethrow 异常
        Supplier<String> bridgedSupplier = FunctionUtils.uncheckedSupplier(flinkSupplier);

        // 2. 将“伪装”后的 Supplier 传给标准 API
        System.out.println("[M3] 演示桥接器 (将会失败):");
        CompletableFuture<String> future = CompletableFuture.supplyAsync(bridgedSupplier);

        try {
            // .get() 会阻塞并等待结果
            // 因为 Flink 的 Lambda 抛出了异常，
            // bridgedSupplier 内部的 rethrow 会把
            // IOException -> RuntimeException
            // CompletableFuture 会捕获这个 RuntimeException
            // 并把它包装在 CompletionException 中抛出
            String result = future.get();
            System.out.println("CompletableFuture 成功: " + result);
        } catch (Exception e) {
            // e 是 ExecutionException 或 InterruptedException
            System.out.println("CompletableFuture 捕获到异常: " + e.getClass().getName());

            // 异常链：CompletionException -> RuntimeException -> IOException
            Throwable cause = e.getCause();
            System.out.println("  -> 原因是: " + cause.getClass().getName());
            System.out.println("    -> 原始 Flink 异常: " + cause.getCause().getClass().getName());
            System.out.println("    -> 原始消息: " + cause.getCause().getMessage());
        }

        // 3. 演示 Consumer
        System.out.println("[M3] 演示 'uncheckedConsumer':");
        ThrowingConsumer<String, IOException> flinkConsumer = (s) -> {
            if (s.isEmpty()) throw new IOException("Consumer 发现空字符串!");
            System.out.println("消费: " + s);
        };

        // “解包”
        Consumer<String> bridgedConsumer = FunctionUtils.uncheckedConsumer(flinkConsumer);

        try {
            List<String> data = Arrays.asList("A", "B", "");
            // Stream.forEach 只接受标准的 Consumer
            data.forEach(bridgedConsumer); // 会在 "" 处抛出异常
        } catch (RuntimeException e) {
            // 异常链：RuntimeException -> IOException
            System.out.println("Stream 捕获到异常: " + e.getClass().getName());
            System.out.println("  -> 原始 Flink 异常: " + e.getCause().getClass().getName());
            System.out.println("  -> 原始消息: " + e.getCause().getMessage());
        }
    }
}
