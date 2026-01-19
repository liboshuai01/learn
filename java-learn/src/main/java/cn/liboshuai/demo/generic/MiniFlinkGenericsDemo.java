package cn.liboshuai.demo.generic;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * <h1>模拟Flink泛型实战Demo</h1>
 * <p>
 * 目标：不依赖任何Flink包，纯Java模拟Flink DataStream API中
 * 关键的泛型设计，帮助理解Flink源码。
 * <p>
 * 我们将模拟:
 * 1. DataStream<T>
 * 2. MapFunction<IN, OUT>
 * 3. SinkFunction<IN>
 * 4. Flink如何使用 ? extends T (PECS - 生产者)
 * 5. Flink如何使用 ? super T (PECS - 消费者)
 * 6. Flink如何"对抗"类型擦除 (TypeInformation 与 .returns())
 */
public class MiniFlinkGenericsDemo {

    // ------------------------------------------------------------------------
    // 1. 模拟 Flink 的核心接口
    // ------------------------------------------------------------------------

    /**
     * 模拟 MapFunction
     *
     * @param <IN>  输入类型
     * @param <OUT> 输出类型
     */
    @FunctionalInterface
    public interface MapFunction<IN, OUT> {
        OUT map(IN value) throws Exception;
    }

    /**
     * 模拟 SinkFunction
     *
     * @param <IN> 输入类型
     */
    @FunctionalInterface
    public interface SinkFunction<IN> {
        void invoke(IN value) throws Exception;
        // 注意：Sink是一个消费者，它只"吃"数据，不产出
    }

    // ------------------------------------------------------------------------
    // 2. 模拟 Flink 的 "TypeInformation"
    //    (对抗类型擦除的关键)
    // ------------------------------------------------------------------------

    /**
     * 模拟 Flink 的 TypeInformation。
     * 在 Flink 中，这是一个非常复杂的类。
     * 在我们这里，我们把它简化为只携带 Class 信息，
     * 它的核心作用就是：在运行时，依然能拿到 <T> 的具体类型。
     *
     * @param <T>
     */
    public static class TypeInformation<T> {
        private final Class<T> typeClass;

        public TypeInformation(Class<T> typeClass) {
            this.typeClass = typeClass;
        }

        public Class<T> getTypeClass() {
            return typeClass;
        }

        // Flink 中有类似 `Types.STRING`, `Types.INT` 的静态实例
        public static final TypeInformation<String> STRING = new TypeInformation<>(String.class);
        public static final TypeInformation<Integer> INT = new TypeInformation<>(Integer.class);
        public static final TypeInformation<Event> EVENT = new TypeInformation<>(Event.class);

        // 模拟 Flink 的 TypeInformation.of(Class)
        public static <T> TypeInformation<T> of(Class<T> clazz) {
            return new TypeInformation<>(clazz);
        }

        @Override
        public String toString() {
            return "TypeInfo<" + typeClass.getSimpleName() + ">";
        }
    }

    // ------------------------------------------------------------------------
    // 3. 模拟 Flink 的 DataStream<T> 核心类
    // ------------------------------------------------------------------------

    /**
     * 模拟 Flink 的 DataStream<T>
     *
     * @param <T> 流中元素的类型
     */
    public static class DataStream<T> {

        // Flink 在运行时必须知道T的具体类型，所以它总会持有一个TypeInformation
        private final TypeInformation<T> typeInfo;

        // 模拟 Source 创建
        public DataStream(TypeInformation<T> typeInfo) {
            this.typeInfo = typeInfo;
            System.out.println("[创建流] " + this);
        }

        public TypeInformation<T> getTypeInfo() {
            return typeInfo;
        }

        /**
         * 模拟 map 算子。
         * 这是泛型方法的核心应用。
         *
         * @param mapFunction 用户的map逻辑
         * @param <R>         输出的新类型
         * @return 一个"待定"的操作，需要用户确认类型
         */
        public <R> PendingOperation<T, R> map(MapFunction<T, R> mapFunction) {
            System.out.println("[调用map] 准备转换 " + this.typeInfo + " -> ?");
            // Flink 在这里会尝试"自动推断" R 的类型
            // 如果 mapFunction 是匿名内部类，Flink可以通过反射成功推断
            // 如果 mapFunction 是 Lambda，Flink 1.18 (Java 8) 很大几率推断失败
            // 所以 Flink 返回一个中间对象，让你有机会 .returns()
            return new PendingOperation<>(this, mapFunction);
        }

        /**
         * 模拟 addSink 算子。
         * 这是 ? super T (消费者) 的经典应用。
         *
         * @param sinkFunction 用户的sink逻辑
         */
        public void addSink(SinkFunction<? super T> sinkFunction) {
            // 为什么是 ? super T ?
            // 假设我们有一个 DataStream<Dog> (T=Dog)
            // 我们可以用 SinkFunction<Dog> (T) 来消费
            // 我们也可以用 SinkFunction<Animal> (T的父类) 来消费
            // 我们甚至可以用 SinkFunction<Object> (T的父类) 来消费
            // 所以，sinkFunction 接受 T 或者 T 的任何父类，即 ? super T
            System.out.println("[调用sink] " + this.typeInfo + " 将被 "
                    + sinkFunction.getClass().getSimpleName() + " 消费");
        }

        /**
         * 模拟 union 算子。
         * 这是 ? extends T (生产者) 的经典应用。
         *
         * @param others 其他要合并的流
         */
        public DataStream<T> union(DataStream<? extends T>... others) {
            // 为什么是 ? extends T ?
            // 假设我们有一个 DataStream<Animal> (T=Animal)
            // 我们可以和 DataStream<Dog> (T的子类) 合并
            // 我们可以和 DataStream<Cat> (T的子类) 合并
            // 合并后的流，统一都是 DataStream<Animal>
            // other 流作为生产者，提供 T 的子类，所以是 ? extends T
            System.out.println("[调用union] " + this.typeInfo + " 准备合并...");
            for (DataStream<? extends T> other : others) {
                System.out.println("  -> 合并 " + other.typeInfo);
            }
            // 返回一个合并后的新流 (这里简化为返回自己)
            return this;
        }

        @Override
        public String toString() {
            return "DataStream<" + (typeInfo != null ? typeInfo.typeClass.getSimpleName() : "?") + ">";
        }
    }

    // ------------------------------------------------------------------------
    // 4. 模拟 Flink 的 "PendingOperation" (为了 .returns())
    // ------------------------------------------------------------------------

    /**
     * 模拟 Flink (如 SingleOutputStreamOperator)
     * 这是一个中间类，用于处理 .returns()
     *
     * @param <IN>  输入类型
     * @param <OUT> 输出类型
     */
    public static class PendingOperation<IN, OUT> {
        private final DataStream<IN> inputStream;
        private final MapFunction<IN, OUT> mapFunction;

        public PendingOperation(DataStream<IN> inputStream, MapFunction<IN, OUT> mapFunction) {
            this.inputStream = inputStream;
            this.mapFunction = mapFunction;
        }

        /**
         * 模拟 Flink 自动类型推断 (主要靠匿名内部类)
         * 这是 "对抗" 类型擦除的第一道防线
         */
        public DataStream<OUT> attemptAutoType() {
            System.out.println("  [类型推断] 尝试从 " + mapFunction.getClass() + " 自动推断输出类型...");
            TypeInformation<OUT> outType = TypeExtractor.getMapOutType(mapFunction);
            if (outType == null) {
                System.err.println("  [类型推断] 失败! 无法从 Lambda 或泛型类推断。");
                throw new IllegalStateException("自动类型推断失败! 请使用 .returns() 显式指定类型。");
            }
            System.out.println("  [类型推断] 成功! 推断输出类型为: " + outType);
            return new DataStream<>(outType);
        }

        /**
         * 模拟 .returns()
         * 这是 "对抗" 类型擦除的第二道防线 (用户手动指定)
         */
        public DataStream<OUT> returns(TypeInformation<OUT> outType) {
            System.out.println("  [类型指定] 用户通过 .returns() 显式指定输出类型: " + outType);
            return new DataStream<>(outType);
        }
    }

    // ------------------------------------------------------------------------
    // 5. 模拟 Flink 的 "TypeExtractor" (反射工具)
    // ------------------------------------------------------------------------

    /**
     * 模拟 Flink 的类型提取器 (TypeExtractor)
     * 专门用反射来"扒"出匿名内部类实现的泛型接口的具体类型
     */
    public static class TypeExtractor {
        /**
         * 尝试获取 MapFunction<IN, OUT> 中的 OUT 类型
         */
        @SuppressWarnings("unchecked")
        public static <OUT> TypeInformation<OUT> getMapOutType(MapFunction<?, ?> mapFunction) {
            // Flink 的 Lambda 检查非常复杂，这里我们简单模拟：
            // 如果是 Lambda (Java 8 会生成一个合成类)，它的泛型信息会被擦除得更干净
            if (mapFunction.getClass().isSynthetic()) {
                return null; // 模拟 Lambda 推断失败
            }

            try {
                // 遍历该类实现的所有接口
                for (Type genericInterface : mapFunction.getClass().getGenericInterfaces()) {
                    if (genericInterface instanceof ParameterizedType) {
                        ParameterizedType pType = (ParameterizedType) genericInterface;
                        // 找到 MapFunction 接口
                        if (pType.getRawType().equals(MapFunction.class)) {
                            // 获取泛型参数列表 <IN, OUT>
                            Type[] typeArgs = pType.getActualTypeArguments();
                            if (typeArgs.length == 2) {
                                // 第 1 个(索引0)是 IN, 第 2 个(索引1)是 OUT
                                Class<OUT> outClass = (Class<OUT>) typeArgs[1];
                                return TypeInformation.of(outClass);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Flink 中有复杂的 CGLIB、TypeVariable 等处理，这里简化
                return null;
            }
            return null;
        }
    }


    // ------------------------------------------------------------------------
    // 6. 我们的 Demo 业务 POJO 和 Main 方法
    // ------------------------------------------------------------------------

    // 业务POJO
    public static class Event {
        public int id;
        public long timestamp;

        public Event(int id) {
            this.id = id;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // Event 的子类
    public static class Alert extends Event {
        public String msg;

        public Alert(int id, String msg) {
            super(id);
            this.msg = msg;
        }
    }

    // Sink 的具体实现
    public static class EventSink implements SinkFunction<Event> {
        @Override
        public void invoke(Event value) { /* 模拟写入外部系统 */ }
    }

    public static class ObjectSink implements SinkFunction<Object> {
        @Override
        public void invoke(Object value) { /* 模拟写入外部系统 */ }
    }


    /**
     * ==========================================================
     * 运行主程序：演示泛型的应用
     * ==========================================================
     */
    public static void main(String[] args) throws Exception {

        // 1. 创建一个源流，类型为 Event
        // Flink 在 Source 处就需要知道类型
        DataStream<Event> eventStream = new DataStream<>(TypeInformation.EVENT);

        System.out.println("\n--- 演示1: map (使用匿名内部类，自动推断类型) ---");
        // Flink 可以通过反射 "扒" 出匿名内部类的泛型签名
        DataStream<String> stringStream = eventStream.map(
                new MapFunction<Event, String>() {
                    @Override
                    public String map(Event value) {
                        return "EventID:" + value.id;
                    }
                }
        ).attemptAutoType(); // 尝试自动推断，应该会成功
        System.out.println("--> 成功创建新流: " + stringStream);


        System.out.println("\n--- 演示2: map (使用Lambda，自动推断失败，必须 .returns()) ---");
        // Lambda 表达式的类型擦除更彻底，Flink 无法推断
        DataStream<Integer> intStream;
        try {
            intStream = eventStream.map(event -> event.id).attemptAutoType();
        } catch (IllegalStateException e) {
            System.err.println("错误捕获: " + e.getMessage());

            // 这就是为什么 Flink 强烈建议对 Lambda 使用 .returns()
            System.out.println("   [补救] 使用 .returns() 显式指定类型...");
            intStream = eventStream.map(event -> event.id)
                    .returns(TypeInformation.INT); // 手动指定返回类型
            System.out.println("--> 成功创建新流: " + intStream);
        }

        System.out.println("\n--- 演示3: addSink (演示 ? super T 消费者) ---");
        DataStream<Alert> alertStream = new DataStream<>(TypeInformation.of(Alert.class));
        // alertStream 的 T = Alert
        // Alert 继承了 Event, Event 继承了 Object

        // 准备一个 Event Sink (Alert的父类)
        SinkFunction<Event> eventSink = new EventSink();
        // 准备一个 Object Sink (Alert的父类)
        SinkFunction<Object> objectSink = new ObjectSink();

        // DataStream<Alert>.addSink(SinkFunction<? super Alert>)
        // SinkFunction<Event> 是 ? super Alert 的一种
        alertStream.addSink(eventSink);
        // SinkFunction<Object> 也是 ? super Alert 的一种
        alertStream.addSink(objectSink);

        System.out.println("--> addSink 演示完毕，编译通过，符合PECS原则。");


        System.out.println("\n--- 演示4: union (演示 ? extends T 生产者) ---");
        // eventStream 的 T = Event
        // alertStream 的 T = Alert
        // DataStream<Event>.union(DataStream<? extends Event>... others)
        // DataStream<Alert> 是 ? extends Event 的一种
        eventStream.union(alertStream);

        System.out.println("--> union 演示完毕，编译通过，符合PECS原则。");
    }
}
