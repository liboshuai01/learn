package cn.liboshuai.demo.cf;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * CompletableFuture 跟写练习 Demo (基于 Java 8)
 * * 跟着 main 方法的步骤，一步一步取消注释来运行和学习。
 */
public class CompletableFutureDemo {

    // 1. 模拟一个“商店服务”，它提供的方法都很慢
    static class ShopService {

        /**
         * 模拟一个【同步阻塞】的查询，耗时1秒
         * * @param product 商品名
         * @return 价格
         */
        public double getPrice(String product) {
            log("开始查询 " + product + " 的价格...");
            try {
                // 模拟耗时 1 秒
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log(product + " 的价格查询完成。");
            // 假设我们查到了价格
            if ("error-item".equals(product)) {
                // 模拟一个查询失败
                throw new RuntimeException("查询商品价格失败！");
            }
            return 99.9;
        }

        /**
         * 模拟一个【异步】查询，但返回的是“哑巴” Future
         * * @param product 商品名
         * @return 一个 Future
         */
        public Future<Double> getPriceAsyncFuture(String product) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            // 提交任务到线程池，返回一个 Future
            return executor.submit(() -> getPrice(product));
        }

        /**
         * 模拟一个【异步】查询，返回“智能” CompletableFuture
         * * @param product 商品名
         * @return 一个 CompletableFuture
         */
        public CompletableFuture<Double> getPriceAsync(String product) {
            // supplyAsync 帮我们把“耗时任务”丢到 Flink/Java 的通用线程池里
            // 它返回一个“回执单”，承诺稍后会有一个 Double 结果
            return CompletableFuture.supplyAsync(() -> getPrice(product));
        }

        /**
         * 模拟另一个【异步】服务：获取折扣
         */
        public CompletableFuture<Double> getDiscountAsync(double price) {
            return CompletableFuture.supplyAsync(() -> {
                log("开始计算折扣...");
                try {
                    TimeUnit.SECONDS.sleep(1); // 模拟计算折扣也耗时1秒
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log("折扣计算完成。");
                return price * 0.9; // 返回 9 折价格
            });
        }
    }

    // 主函数，我们的练习场
    public static void main(String[] args) throws Exception {

        log("--- Demo 开始 ---");

        // -----------------------------------------------------------------
        // 【第 1 站】感受“哑巴 Future”的阻塞之痛
        // -----------------------------------------------------------------
//         demoBlockingFuture();


        // -----------------------------------------------------------------
        // 【第 2 站】CF 出场：supplyAsync 拿到“智能回执单”，主线程不阻塞
        // -----------------------------------------------------------------
//         demoNonBlockingCF();


        // -----------------------------------------------------------------
        // 【第 3 站】核心：链式调用（thenApply / thenAccept）
        // -----------------------------------------------------------------
//         demoChaining();


        // -----------------------------------------------------------------
        // 【第 4 站】组合：并行（thenCombine）
        // -----------------------------------------------------------------
//         demoCombine();


        // -----------------------------------------------------------------
        // 【第 5 站】核心：串联（thenCompose）
        // -----------------------------------------------------------------
        // demoCompose();


        // -----------------------------------------------------------------
        // 【第 6 站】异常处理（exceptionally）
        // -----------------------------------------------------------------
        // demoExceptionHandling();


        log("--- Demo 结束（主线程即将等待） ---");

        // 【重要！】
        // 因为 CF 默认用的是守护线程（Daemon Thread），
        // 如果主线程（main）退出了，守护线程会立刻被强行关闭，你就看不到异步结果了。
        // 所以我们让主线程睡 10 秒，“等一等”异步的兄弟们。
        log("主线程将休眠 10 秒，等待所有异步任务执行完毕...");
        TimeUnit.SECONDS.sleep(10);
    }

    // --- Demo 1：感受“哑巴 Future”的阻塞 ---
    private static void demoBlockingFuture() throws Exception {
        log("--- 【Demo 1：阻塞的 Future】---");
        ShopService shop = new ShopService();
        long start = System.currentTimeMillis();

        Future<Double> futurePrice = shop.getPriceAsyncFuture("商品A");
        log("拿到了 Future“回执单”，主线程看起来自由了...");

        // ... 主线程干了点别的事 ...
        log("主线程干点别的（比如准备UI）...");
        TimeUnit.MILLISECONDS.sleep(200);

        // 致命点：当主线程【需要】结果时，它只能调用 get() 傻等
        log("主线程准备拿结果... 调用 get() ...");
        double price = futurePrice.get(); // 这一行会卡住主线程！

        log("主线程终于拿到了结果：" + price);
        log("Demo 1 总耗时: " + (System.currentTimeMillis() - start) + "ms");
        // 你会发现，总耗时 > 1000ms，因为 get() 阻塞了
    }

    // --- Demo 2：CF 出场，主线程不阻塞 ---
    private static void demoNonBlockingCF() {
        log("--- 【Demo 2：非阻塞的 CF】---");
        ShopService shop = new ShopService();
        long start = System.currentTimeMillis();

        // 1. 拿到“智能回执单”
        CompletableFuture<Double> cfPrice = shop.getPriceAsync("商品B");
        log("拿到了 CF“回执单”，主线程继续...");

        // 2. 主线程继续干别的，完全没停
        log("主线程干别的（比如准备UI）...");

        log("Demo 2 主线程耗时: " + (System.currentTimeMillis() - start) + "ms");
        // 你会发现，主线程几乎 0 耗时就跑完了。
        // （CF 会在后台的 ForkJoinPool 线程池里默默执行）
    }

    // --- Demo 3：核心：链式调用 ---
    private static void demoChaining() {
        log("--- 【Demo 3：链式调用 thenApply & thenAccept】---");
        ShopService shop = new ShopService();

        shop.getPriceAsync("商品C") // 第1步：异步查价格（返回 CF<Double>）

                .thenApply(price -> { // 第2步：拿到价格 (price)，【同步】处理，返回新结果
                    log("第2步(thenApply): 拿到价格 " + price + "，计算 8 折");
                    return price * 0.8; // 返回一个新价格（Double）
                })

                .thenAccept(finalPrice -> { // 第3步：拿到最终价格 (finalPrice)，【同步】消费，不返回
                    log("第3步(thenAccept): 拿到最终 8 折价 " + finalPrice + "，准备展示");
                });

        log("主线程已“注册”完所有“后续动作”，闪人。");
        // 整个链条会在后台自动依次执行
    }

    // --- Demo 4：组合：并行 ---
    private static void demoCombine() {
        log("--- 【Demo 4：并行组合 thenCombine】---");
        ShopService shop = new ShopService();
        long start = System.currentTimeMillis();

        // 1. 【同时】发起两个异步查询
        log("同时发起 查价格 和 查折扣 两个任务...");
        CompletableFuture<Double> cfPrice = shop.getPriceAsync("商品D");
        // 假设原价是 100，这里为了演示方便写死了，实际应依赖 cfPrice 的结果
        CompletableFuture<Double> cfDiscount = shop.getDiscountAsync(100.0);

        // 2. 【组合】
        // 当 cfPrice 和 cfDiscount 都完成后，把它们的结果(p, d)拿来组合
        cfPrice.thenCombine(cfDiscount, (p, d) -> {
                    log("第2步(thenCombine): 两个任务都完成了，合并结果");
                    // 注意：这里 p=99.9, d=90.0 (因为我们写死了 100 * 0.9)
                    // 只是为了演示两个CF的组合
                    return "原价 " + p + ", 折扣价 " + d;
                })
                .thenAccept(result -> {
                    log("第3步(thenAccept): 拿到合并结果: " + result);
                    log("Demo 4 总耗时: " + (System.currentTimeMillis() - start) + "ms");
                    // 你会发现，总耗时约 1000ms（两个 1 秒的任务并行执行，取最长的那个）
                    // 如果是串行，那就是 2000ms
                });

        log("主线程已“注册”完组合任务，闪人。");
    }

    // --- Demo 5：核心：串联 ---
    private static void demoCompose() {
        log("--- 【Demo 5：核心串联 thenCompose】---");
        ShopService shop = new ShopService();

        // 场景：先查价格，【然后】根据查到的价格再去查折扣

        shop.getPriceAsync("商品E") // 第1步：异步查价格（返回 CF<Double>）

                // thenCompose：接收上一步的结果 (price)，
                // 但【必须】返回一个新的“回执单” (CF<Double>)
                .thenCompose(price -> {
                    log("第2步(thenCompose): 拿到价格 " + price + "，开始异步查折扣");
                    // 返回【新的】异步任务
                    return shop.getDiscountAsync(price);
                })

                .thenAccept(finalDiscountedPrice -> { // 第3步：拿到【最终】折扣价
                    log("第3步(thenAccept): 拿到了【最终】的折扣价 " + finalDiscountedPrice);
                });

        log("主线程已“注册”完串联任务，闪人。");
        // 你会发现，这个总耗时约 2000ms，因为两个 1 秒的任务是【串行】的
    }

    // --- Demo 6：异常处理 ---
    private static void demoExceptionHandling() {
        log("--- 【Demo 6：异常处理 exceptionally】---");
        ShopService shop = new ShopService();

        // 我们故意查一个会“失败”的商品
        shop.getPriceAsync("error-item") // 第1步：异步查价格（这步会抛异常）

                .thenApply(price -> { // 第2步：计算 8 折
                    // 【注意】由于上一步出错了，这一步(thenApply)会被【跳过】
                    log("第2步(thenApply): 这一步【不会】执行！");
                    return price * 0.8;
                })

                .exceptionally(ex -> { // 第3步：异常“兜底”
                    log("第3步(exceptionally): 出错了！错误信息: " + ex.getMessage());
                    return 0.0; // 返回一个“兜底”的默认价格 0.0
                })

                .thenAccept(finalPrice -> { // 第4步：
                    // 这一步会正常执行，拿到的是“兜底”的 0.0
                    log("第4步(thenAccept): 拿到最终价格（兜底的）: " + finalPrice);
                });

        log("主线程已“注册”完带异常处理的链条，闪人。");
    }


    // 辅助方法：打印日志（带线程名）
    private static void log(String message) {
        System.out.println("[" + Thread.currentThread().getName() + "] " + message);
    }
}
