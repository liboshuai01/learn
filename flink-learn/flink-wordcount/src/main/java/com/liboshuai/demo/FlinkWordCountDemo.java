package com.liboshuai.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 单词计数示例
 */
public class FlinkWordCountDemo {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkWordCountDemo.class);

    // 定义参数键
    private static final String KEY_HOSTNAME = "hostname";
    private static final String KEY_PORT = "port";
    private static final String PARALLELISM = "parallelism";

    // 定义默认值
    private static final String DEFAULT_HOSTNAME = "localhost";
    private static final int DEFAULT_PORT = 9999;
    private static final int DEFAULT_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        // 1. 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 使用 ParameterTool 解析命令行参数
        // 示例: --hostname my-host --port 12345
        final ParameterTool params = ParameterTool.fromArgs(args);
        // 检查是否提供了必要的参数，并提供友好的提示
        if (args.length == 0) {
            LOG.warn("未提供任何命令行参数，将使用默认主机、端口、并行度: {}:{}:{}", DEFAULT_HOSTNAME, DEFAULT_PORT, DEFAULT_PARALLELISM);
        }

        // 3. 设置 flink 配置
//        setConfig(env, params);

        // 4. 构建并执行 Flink 作业
        buildAndExecuteJob(env, params);
    }

    /**
     * 设置 Flink 的 checkpoint 配置
     */
    private static void setConfig(StreamExecutionEnvironment env, ParameterTool params) {
        // 设置全局并行度
        int parallelism = params.getInt(PARALLELISM, DEFAULT_PARALLELISM);
        env.setParallelism(parallelism);

        // 启用 checkpoint，并设置时间间隔
        env.enableCheckpointing(10000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 两次 checkpoint 之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(10000);
        // checkpoint 超时时间
        checkpointConfig.setCheckpointTimeout(10000);
        // 作业手动取消时，保留 checkpoint 数据（需手动清理）
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpoint 语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 最多允许 checkpoint 失败次数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        // 同一时间只允许一个 checkpoint 进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 设置 checkpoint 存储位置（要与 flink on k8s 挂载的pvc目录路径一致）
        checkpointConfig.setCheckpointStorage("file:///opt/flink/checkpoints");
//        checkpointConfig.setCheckpointStorage("file:///C:/Me/Temp/flink/checkpoint");
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
    }

    /**
     * 构建 Flink 数据处理管道并执行。
     *
     * @param env    Flink 流执行环境
     * @param params 命令行参数工具
     */
    private static void buildAndExecuteJob(StreamExecutionEnvironment env, ParameterTool params) throws Exception {
        String hostname = params.get(KEY_HOSTNAME, DEFAULT_HOSTNAME);
        int port = params.getInt(KEY_PORT, DEFAULT_PORT);
        int parallelism = params.getInt(PARALLELISM, DEFAULT_PARALLELISM);

        LOG.info("开始构建 Flink 作业，数据源 Socket: {}:{}，并行度: {}", hostname, port, parallelism);

        // 4. 创建数据源：从 Socket 读取文本流
        DataStream<String> textStream = env.socketTextStream(hostname, port)
                .name("socket-数据源");

        // 5. 执行转换操作 (ETL)
        DataStream<Tuple2<String, Integer>> counts = textStream
                // 使用 Lambda 表达式进行 FlatMap, 更简洁
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    // 按非字母数字字符分割
                    String[] tokens = line.toLowerCase().split("\\W+");
                    for (String token : tokens) {
                        if (!token.isEmpty()) {
                            out.collect(new Tuple2<>(token, 1));
                        }
                    }
                })
                // [最佳实践] 显式提供类型信息，避免 Java 类型擦除导致的问题
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .name("分词器(Tokenizer)")
                .keyBy(value -> value.f0) // 根据单词 (Tuple 的第0个元素) 进行分组
                .sum(1) // 对计数值 (Tuple 的第1个元素) 进行累加
                .name("单词聚合器(Aggregator)");

        // 6. 添加 Sink：将结果使用自定义的 SinkFunction 打印到日志
        counts.addSink(new LogPrintSink<>()).name("日志打印(Sink)");

        // 7. 执行 Flink 作业，并指定一个有意义的作业名称
        LOG.info("作业图构建完成，开始执行 Flink 作业...");
        env.execute("Flink WordCount 示例 (Socket 输入)");
    }

    /**
     * 自定义的 SinkFunction，用于将数据流中的每个元素通过 SLF4J/Log4j2 打印出来。
     *
     * @param <T> 数据类型
     */
    public static class LogPrintSink<T> implements SinkFunction<T> {

        @Override
        public void invoke(T value, Context context) throws Exception {
            // 使用中文日志模板记录每个到达 Sink 的元素
            LOG.info("Flink 计算结果: {}", value);
        }
    }
}