package com.liboshuai.demo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * CompletableFuture 快速入门指南 (Java 1.8)
 * <p>
 * 本指南通过一系列模拟生产环境的场景，展示 CompletableFuture 的核心用法。
 * 每个 "场景" 都是一个可独立运行的 main 方法，建议按顺序学习。
 *
 * @author Your Guide
 */
public class CompletableFutureQuickStartGuide {

    // 模拟一个RPC/数据库调用的线程池，生产环境中应当自定义并精细化配置
    private static final ExecutorService bizExecutor = Executors.newFixedThreadPool(10, r -> {
        Thread thread = new Thread(r);
        thread.setName("业务线程-" + thread.getId());
        return thread;
    });

    /**
     * 模拟一个耗时操作，例如一次RPC调用、数据库查询等
     *
     * @param operationName 操作名称
     * @param seconds       耗时（秒）
     * @return 操作结果
     */
    private static String mockRpcCall(String operationName, int seconds) {
        try {
            System.out.printf("线程 [%s] 开始执行耗时操作: %s...%n", Thread.currentThread().getName(), operationName);
            TimeUnit.SECONDS.sleep(seconds);
            System.out.printf("线程 [%s] 完成了耗时操作: %s%n", Thread.currentThread().getName(), operationName);
            return String.format("'%s' 的结果", operationName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("模拟操作被中断", e);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("--- 场景1: 异步执行一个耗时任务 ---");
        scene1_runAsync();
        Thread.sleep(2100); // 等待异步任务执行完毕

//        System.out.println("\n--- 场景2: 异步执行并获取返回值，并对结果进行转换 ---");
//        scene2_thenApply();
//
//        System.out.println("\n--- 场景3: 消费上一步结果，执行一个动作 (无返回值) ---");
//        scene3_thenAccept();
//        Thread.sleep(2100);
//
//        System.out.println("\n--- 场景4: RPC风格的链式调用 (关键API: thenCompose) ---");
//        scene4_thenCompose();
//
//        System.out.println("\n--- 场景5: 并行执行两个任务，并在都完成后组合结果 ---");
//        scene5_thenCombine();
//
//        System.out.println("\n--- 场景6: 处理异步任务中的异常 ---");
//        scene6_exceptionally();
//
//        System.out.println("\n--- 场景7: 竞赛模式，谁快用谁的结果 ---");
//        scene7_applyToEither();
//
//        System.out.println("\n--- 最终整合场景: 模拟一个复杂的用户看板数据加载流程 ---");
//        scene8_comprehensive();

        // 在实际应用中 (如Web服务器)，不需要手动关闭线程池
        // 这里为了演示程序能正常退出而关闭
        bizExecutor.shutdown();
    }

    /**
     * 场景1: 提交一个异步任务，不关心返回结果。
     * API: runAsync(Runnable)
     */
    public static void scene1_runAsync() {
        // 使用 runAsync 异步执行一个没有返回值的任务
        CompletableFuture.runAsync(() -> {
            mockRpcCall("异步记录日志", 2);
        }, bizExecutor);
        System.out.println("主线程继续执行其他任务，日志记录已在后台异步进行...");
    }

    /**
     * 场景2: 异步获取一个结果，并对结果进行同步转换。
     * API: supplyAsync(Supplier), thenApply(Function)
     * thenApply: 当上一步完成后，使用其结果作为输入，执行一个【同步】的转换函数，并返回一个新的CompletableFuture。
     *          注意：thenApply中的代码会由执行上一步的线程或者主线程来执行，它本身不是异步的。
     */
    public static void scene2_thenApply() {
        CompletableFuture<String> userInfoFuture = CompletableFuture.supplyAsync(() -> {
            return mockRpcCall("获取用户信息", 2);
        }, bizExecutor);

        // 使用 thenApply 对上一步的结果进行加工
        CompletableFuture<String> welcomeMessageFuture = userInfoFuture.thenApply(userInfo -> {
            System.out.printf("线程 [%s] 正在加工用户信息...%n", Thread.currentThread().getName());
            return "欢迎您, " + userInfo;
        });

        System.out.println("主线程已提交任务，现在等待最终结果...");
        // .join() 是一个阻塞方法，会等待 Future 完成并返回结果
        String result = welcomeMessageFuture.join();
        System.out.println("主线程获取到最终结果: " + result);
    }

    /**
     * 场景3: 异步获取一个结果，并消费它，不产生新的返回值。
     * API: thenAccept(Consumer)
     */
    public static void scene3_thenAccept() {
        CompletableFuture<Void> notificationFuture = CompletableFuture.supplyAsync(() ->
                        mockRpcCall("获取用户手机号", 2), bizExecutor)
                .thenAccept(phoneNumber -> {
                    // thenAccept 用于消费上一步的结果，它本身没有返回值 (返回 CompletableFuture<Void>)
                    System.out.printf("线程 [%s] 正在发送短信给: %s%n", Thread.currentThread().getName(), phoneNumber);
                });

        System.out.println("主线程已提交发送短信任务，不关心发送结果...");
        // 这里为了演示，主线程可以继续做别的事
    }

    /**
     * 场景4: 异步任务的链式调用，这是 Flink RPC 或任何服务间异步调用的核心。
     * 需求: 1. 先异步获取用户ID -> 2. 根据用户ID再异步获取用户订单。
     * 关键API: thenCompose(Function<T, CompletionStage<U>>)
     * <p>
     * 对比 thenApply:
     * - thenApply(Function<T, U>): 输入T，返回一个普通值U。用于同步转换。
     * - thenCompose(Function<T, CompletionStage<U>>): 输入T，返回一个 CompletableFuture<U>。用于将两个异步操作连接起来。
     * 使用 thenApply 会导致 CompletableFuture<CompletableFuture<Order>> 的嵌套地狱。
     */
    public static void scene4_thenCompose() {
        CompletableFuture<String> finalResultFuture = CompletableFuture.supplyAsync(() -> {
            // 第一个异步调用：获取用户ID
            return mockRpcCall("获取用户ID", 1);
        }, bizExecutor).thenCompose(userId -> {
            // thenCompose接收上一步的结果(userId), 返回一个新的CompletableFuture
            // 这使得我们可以将两个异步调用平滑地串联起来
            System.out.printf("线程 [%s] 拿到了用户ID: %s, 准备发起第二次调用获取订单...%n", Thread.currentThread().getName(), userId);
            // 第二个异步调用：根据用户ID获取订单
            return CompletableFuture.supplyAsync(() -> mockRpcCall("获取 " + userId + " 的订单", 2), bizExecutor);
        });

        System.out.println("主线程已提交【获取用户->获取订单】的链式任务...");
        System.out.println("主线程最终获取到结果: " + finalResultFuture.join());
    }

    /**
     * 场景5: 并行执行两个独立的异步任务，并在它们都完成后，合并结果。
     * 需求: 同时获取用户的 "基本信息" 和 "账户余额"，然后合并成一个视图对象。
     * API: thenCombine(CompletionStage, BiFunction)
     */
    public static void scene5_thenCombine() {
        // 任务1: 异步获取用户基本信息
        CompletableFuture<String> profileFuture = CompletableFuture.supplyAsync(() ->
                mockRpcCall("获取用户基本信息", 2), bizExecutor);

        // 任务2: 异步获取用户账户余额
        CompletableFuture<String> balanceFuture = CompletableFuture.supplyAsync(() ->
                mockRpcCall("获取用户账户余额", 3), bizExecutor);

        System.out.println("主线程提交了两个并行的任务：获取用户信息和余额...");

        // 使用 thenCombine 合并两个任务的结果
        CompletableFuture<String> combinedFuture = profileFuture.thenCombine(balanceFuture, (profile, balance) -> {
            System.out.printf("线程 [%s] 正在合并用户信息和余额...%n", Thread.currentThread().getName());
            return "用户信息: [" + profile + "] | 账户余额: [" + balance + "]";
        });

        System.out.println("主线程等待合并结果...");
        System.out.println("主线程获取到最终合并结果: " + combinedFuture.join());
    }

    /**
     * 场景6: 优雅地处理异步执行过程中的异常。
     * 需求: 获取用户信息时可能抛出异常 (如用户不存在)，此时我们不希望整个流程中断，而是返回一个默认的 "游客" 信息。
     * API: exceptionally(Function)
     */
    public static void scene6_exceptionally() {
        CompletableFuture<String> userFuture = CompletableFuture.supplyAsync(() -> {
            // 模拟一个有50%几率失败的操作
            if (ThreadLocalRandom.current().nextBoolean()) {
                throw new RuntimeException("数据库连接超时，无法获取用户信息");
            }
            return mockRpcCall("获取尊贵VIP用户信息", 1);
        }, bizExecutor).exceptionally(ex -> {
            // exceptionally 捕获上一步的异常，并提供一个备用结果
            System.err.printf("线程 [%s] 捕获到异常: %s, 返回默认值%n", Thread.currentThread().getName(), ex.getMessage());
            return "默认游客信息";
        });

        System.out.println("主线程已提交任务，并设置了异常处理...");
        System.out.println("主线程获取到的用户信息: " + userFuture.join());
    }

    /**
     * 场景7: 竞赛模式，两个任务并行，谁先完成就用谁的结果。
     * 需求: 从两个不同的API源获取商品价格，使用先返回的那个。
     * API: applyToEither(CompletionStage, Function)
     */
    public static void scene7_applyToEither() {
        CompletableFuture<String> sourceAFuture = CompletableFuture.supplyAsync(() ->
                        mockRpcCall("从数据源A获取价格", ThreadLocalRandom.current().nextInt(1, 4)),
                bizExecutor
        );

        CompletableFuture<String> sourceBFuture = CompletableFuture.supplyAsync(() ->
                        mockRpcCall("从数据源B获取价格", ThreadLocalRandom.current().nextInt(1, 4)),
                bizExecutor
        );

        System.out.println("主线程已提交两个竞速任务...");

        // applyToEither: 将一个函数应用到两个CF中先完成的那一个的结果上。
        CompletableFuture<String> finalPriceFuture = sourceAFuture.applyToEither(sourceBFuture, price -> {
            System.out.printf("线程 [%s] 正在处理胜出的结果...%n", Thread.currentThread().getName());
            return "最终采纳价格: " + price;
        });

        System.out.println("主线程最终获取的结果: " + finalPriceFuture.join());
    }

    /**
     * 最终整合场景: 模拟一个复杂的用户看板数据加载流程
     * 1. 异步获取用户ID
     * 2. 并行获取用户的 "订单列表" 和 "优惠券列表"
     * 3. 在两者都获取到后，将它们组合起来
     * 4. 如果中途有任何异常，则返回一个 "加载失败" 的提示
     * 5. 最后，将看板信息展示出来
     */
    public static void scene8_comprehensive() {
        // 开始节点
        CompletableFuture<String> start = CompletableFuture.completedFuture("user-123");

        CompletableFuture<String> dashboardFuture = start
                // 1. 链式调用：拿到ID后，触发两个并行任务
                .thenCompose(userId -> {
                    System.out.printf("线程 [%s] 获取到用户ID: %s, 开始并行加载订单和优惠券%n", Thread.currentThread().getName(), userId);

                    // 2a. 并行任务一: 获取订单列表
                    CompletableFuture<String> ordersFuture = CompletableFuture.supplyAsync(
                            () -> mockRpcCall("获取 " + userId + " 的订单列表", 2), bizExecutor);

                    // 2b. 并行任务二: 获取优惠券列表
                    CompletableFuture<String> couponsFuture = CompletableFuture.supplyAsync(
                            () -> mockRpcCall("获取 " + userId + " 的优惠券列表", 3), bizExecutor);

                    // 模拟优惠券服务异常
                    if (ThreadLocalRandom.current().nextBoolean()) {
                        couponsFuture = CompletableFuture.supplyAsync(() -> {
                            throw new RuntimeException("优惠券服务暂时不可用");
                        });
                    }


                    // 3. 组合两个并行任务的结果
                    return ordersFuture.thenCombine(couponsFuture, (orders, coupons) -> {
                        System.out.printf("线程 [%s] 成功组合了订单和优惠券信息%n", Thread.currentThread().getName());
                        return "看板数据 -> [ " + orders + " | " + coupons + " ]";
                    });
                })
                // 4. 设置全局异常处理
                .exceptionally(ex -> {
                    System.err.printf("线程 [%s] 在加载看板数据时发生严重错误: %s%n", Thread.currentThread().getName(), ex.getMessage());
                    return "看板数据加载失败，请稍后重试！";
                });

        // 5. 最终消费结果
        dashboardFuture.thenAccept(dashboardData -> {
            System.out.println("================= 用户看板最终呈现 ==================");
            System.out.println(dashboardData);
            System.out.println("===================================================");
        }).join(); // join() 等待最终的accept执行完成
    }
}
