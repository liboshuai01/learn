package cn.liboshuai.demo.optional;

import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections; // 用于 Collections.emptyList()
import java.util.Arrays;

public class OptionalChapter6 {

    // --- 辅助类和方法 ---

    // 一个简单的 User 类
    static class User {
        private String name;
        User(String name) { this.name = name; }
        public String getName() { return name; }
    }

    // --- 1. 良好实践: 作为方法返回值 ---
    // (替代了返回 null)
    public static Optional<User> findUserById(long id) {
        if (id == 1L) {
            return Optional.of(new User("AdminUser (ID: 1)"));
        }
        // 我们没有找到用户，返回一个清晰的“空”Optional，而不是 null
        return Optional.empty();
    }


    // --- 2. 反模式: 作为方法参数 ---

    // 差的实践 (Bad): 强迫调用者包装参数
    public static void processUserBad(Optional<User> userOpt) {
        // 方法内部也必须解包
        String name = userOpt.map(User::getName).orElse("Guest");
        System.out.println("  -> (Bad) 正在处理: " + name);
    }

    // 好的实践 (Good): 使用方法重载
    public static void processUserGood(User user) {
        // 明确处理非空用户
        System.out.println("  -> (Good) 正在处理: " + user.getName());
    }
    public static void processUserGood() {
        // 明确处理“缺失”情况
        System.out.println("  -> (Good) 正在处理: Guest");
    }


    // --- 3. 反模式: 包装集合 ---

    // 差的实践 (Bad): 返回 Optional<List<T>>
    public static Optional<List<String>> getNamesBad(boolean found) {
        if (found) {
            return Optional.of(Arrays.asList("Flink", "Kafka"));
        }
        // 这非常糟糕，调用者必须处理 Optional，然后再处理空 List 的可能
        return Optional.empty();
    }

    // 好的实践 (Good): 返回空集合
    public static List<String> getNamesGood(boolean found) {
        if (found) {
            return Arrays.asList("Flink", "Kafka");
        }
        // 关键：返回一个空的集合，而不是 null 或 Optional.empty()
        return Collections.emptyList();
    }


    // --- 4. 演示 (main 方法) ---
    public static void main(String[] args) {

        System.out.println("--- 1. 演示 (Good): 作为方法返回值 & 函数式使用 ---");
        // 调用者可以优雅地处理“存在”与“缺失”
        String username1 = findUserById(1L)
                .map(User::getName)
                .orElse("Default User");
        System.out.println("ID 1L 结果: " + username1);

        String username2 = findUserById(2L) // ID 2 不存在
                .map(User::getName)
                .orElse("Default User");
        System.out.println("ID 2L 结果: " + username2);


        System.out.println("\n--- 2. 演示 (Bad): 作为方法参数 ---");
        User myUser = new User("Alice");

        System.out.println("调用 'Bad' 方法 (调用者很繁琐):");
        processUserBad(Optional.of(myUser)); // 必须包装
        processUserBad(Optional.empty());    // 必须用 empty() 代替 null

        System.out.println("\n调用 'Good' 方法 (调用者很清晰):");
        processUserGood(myUser); // 有用户，调用 user 版本
        processUserGood();       // 没用户，调用无参版本


        System.out.println("\n--- 3. 演示 (Bad): 包装集合 ---");
        System.out.println("调用 'Bad' 方法 (found=false):");
        // 调用者必须做两层检查（Optional.isPresent 和 List.isEmpty）
        Optional<List<String>> namesOpt = getNamesBad(false);
        List<String> namesList1 = namesOpt.orElse(Collections.emptyList());
        System.out.println("  -> (Bad) 列表大小: " + namesList1.size());
        // for (String name : namesList1) { ... } // 仍然安全，但获取 List 很麻烦

        System.out.println("\n调用 'Good' 方法 (found=false):");
        // 调用者无需任何检查，可以直接遍历！
        List<String> namesList2 = getNamesGood(false);
        System.out.println("  -> (Good) 列表大小: " + namesList2.size());

        System.out.println("  -> (Good) 尝试安全地遍历空列表:");
        for (String name : namesList2) {
            // 这段代码不会执行，也永远不会抛出 NPE
            System.out.println("  -> (这不会打印) " + name);
        }
        System.out.println("  -> (遍历完成，没有 NPE)");


        System.out.println("\n--- 4. 演示 (Good): 桥接老旧 API (如 Map) ---");
        // Map.get() 返回 null，是典型的老旧 API
        Map<String, String> config = new HashMap<>();
        config.put("flink.version", "1.18");

        // 使用 ofNullable() 将可能为 null 的值安全转换为 Optional
        Optional<String> versionOpt = Optional.ofNullable(config.get("flink.version"));
        Optional<String> missingOpt = Optional.ofNullable(config.get("missing.key"));

        // 然后就可以开始优雅的链式调用
        System.out.println("Version: " + versionOpt.orElse("Unknown"));
        System.out.println("Missing Key: " + missingOpt.orElse("DefaultValue"));
    }
}
