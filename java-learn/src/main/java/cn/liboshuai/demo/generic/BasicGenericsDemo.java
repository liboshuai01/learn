package cn.liboshuai.demo.generic;

/**
 * <h1>Java 泛型基础核心演示</h1>
 * <p>
 * 目标：用最简单的代码演示三个核心概念：
 * 1. 泛型接口 (Generic Interface)
 * 2. 泛型类 (Generic Class)
 * 3. 泛型方法 (Generic Method)
 * <p>
 * 这有助于我们理解 Flink API 的骨架。
 */
public class BasicGenericsDemo {

    // ==========================================================
    // 1. 泛型接口 (Generic Interface)
    // ==========================================================
    /**
     * 泛型接口是 Flink 的基石。
     * 比如 Flink 的 MapFunction<IN, OUT>, SinkFunction<IN> 都是泛型接口。
     *
     * @param <T> 输入类型
     * @param <R> 返回类型
     */
    interface Processor<T, R> {
        R process(T input);
    }

    /**
     * 泛型接口的实现 (实现类)
     * 在实现接口时，我们 "锁定" 了泛型 T 为 String, R 为 Integer
     */
    static class StringToLengthProcessor implements Processor<String, Integer> {
        @Override
        public Integer process(String input) {
            return input == null ? 0 : input.length();
        }
    }


    // ==========================================================
    // 2. 泛型类 (Generic Class)
    // ==========================================================
    /**
     * 泛型类也非常普遍。
     * 比如 Flink 的 DataStream<T>, KeyedStream<T, KEY> 都是泛型类。
     *
     * @param <T> 泛型类持有的元素类型
     */
    static class Container<T> {
        private T item;

        public Container(T item) {
            this.item = item;
        }

        public T getItem() {
            return item;
        }

        public void setItem(T item) {
            this.item = item;
        }

        /**
         * 注意：这个方法不是泛型方法。
         * 它使用的是 *类* 声明时定义的泛型 T。
         */
        public void printItemType() {
            if (item != null) {
                System.out.println("  [泛型类] 容器中持有的项目类型是: " + item.getClass().getName());
            } else {
                System.out.println("  [泛型类] 容器中没有项目。");
            }
        }
    }


    // ==========================================================
    // 3. 泛型方法 (Generic Method)
    // ==========================================================
    /**
     * 这是一个包含泛型方法的工具类
     */
    static class Utils {
        /**
         * 这是一个泛型方法。
         * - 它的 <E> 声明在返回值(void)之前，表示这是一个泛型方法。
         * - 这个 <E> 与它所在的类(Utils)是否是泛型类无关。
         * - 这个 <E> 是在 *调用时* 由编译器根据传入的参数自动推断的。
         *
         * 就像 Flink 的 DataStream.fromElements(T... elements)
         */
        public static <E> void printElement(E element) {
            System.out.println("  [泛型方法] 打印: " + element.toString() +
                    " (实际类型: " + element.getClass().getName() + ")");
        }

        /**
         * 另一个泛型方法示例：返回传入的任意类型
         */
        public static <T> T identity(T item) {
            // 这里可以做一些通用处理...
            return item;
        }
    }


    // ==========================================================
    // 运行 Main 方法
    // ==========================================================
    public static void main(String[] args) {

        System.out.println("--- 1. 泛型接口 (Generic Interface) 演示 ---");
        // 创建一个实现
        Processor<String, Integer> stringProcessor = new StringToLengthProcessor();
        Integer length = stringProcessor.process("Hello Flink");
        System.out.println("处理结果: " + length);

        // 我们也可以使用匿名内部类，这是 Flink 早期版本中很常见的做法
        // 编译器会检查类型，你 new Processor<Integer, String> 就必须实现 process(Integer input)
        Processor<Integer, String> intProcessor = new Processor<Integer, String>() {
            @Override
            public String process(Integer input) {
                return "数字: " + input;
            }
        };
        String result = intProcessor.process(118);
        System.out.println("处理结果: " + result);


        System.out.println("\n--- 2. 泛型类 (Generic Class) 演示 ---");
        // 创建一个持有 String 的容器
        // 编译器会检查，创建时传入 String，getItem() 自动返回 String
        Container<String> stringContainer = new Container<>("这是一段文本");
        stringContainer.printItemType();
        String item1 = stringContainer.getItem(); // 不需要强转
        System.out.println("取出的项目: " + item1);

        // 创建一个持有 Integer 的容器
        Container<Integer> intContainer = new Container<>(12345);
        intContainer.printItemType();
        Integer item2 = intContainer.getItem(); // 不需要强转
        System.out.println("取出的项目: " + item2);

        // 编译时安全：下面这行会报错，防止了 ClassCastException
        // stringContainer.setItem(123); // 编译错误! 类型不匹配


        System.out.println("\n--- 3. 泛型方法 (Generic Method) 演示 ---");
        System.out.println("调用泛型方法 Utils.printElement:");
        // 编译器会根据传入的参数，自动推断出 <E> 的类型
        Utils.printElement("Hello");   // E 被推断为 String
        Utils.printElement(123);       // E 被推断为 Integer
        Utils.printElement(3.14);    // E 被推断为 Double
        Utils.printElement(new Container<>("泛型类实例")); // E 被推断为 Container<String>

        // 调用泛型方法 Utils.identity
        String identityStr = Utils.identity("abc");
        Integer identityInt = Utils.identity(999);
        System.out.println("  [泛型方法] identityStr: " + identityStr);
        System.out.println("  [泛型方法] identityInt: " + identityInt);
    }
}
