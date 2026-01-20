# Flink 实时单词计数示例 (优化版)

## 项目简介

这是一个基于 **Apache Flink** 实现的经典 WordCount 示例项目。该程序从 Socket 文本流中实时读取数据（例如，通过 `netcat` 输入的字符串），进行单词切分、计数统计，并将结果实时打印到日志中。

本项目代码经过了精心优化，旨在展示 Flink 应用开发的**最佳实践**，包括灵活的参数管理、现代化的代码风格和健壮的类型处理，是学习 Flink 数据流处理的绝佳入门示例。

## 技术栈

- **计算引擎**: Apache Flink
- **编程语言**: Java 11+
- **构建工具**: Maven
- **日志框架**: SLF4J + Logback/Log4j2

## 核心特性与优化点

- **✅ 灵活的参数管理**: 使用 Flink 的 `ParameterTool` 进行灵活的命令行参数管理（如 `--hostname`, `--port`），而非硬编码或简单的数组索引。
- **✅ 现代化的代码风格**: 采用 Java Lambda 表达式简化函数式接口的实现，使代码更简洁、更具可读性。
- **✅ 健壮的类型处理**: 显式使用 `.returns()` 为 Lambda 表达式指定返回类型信息，有效避免了 Java 类型擦除可能导致的运行时错误。
- **✅ 清晰的代码结构**: 将核心的数据处理逻辑与 `main` 函数的环境设置分离，提升了代码的可读性与可维护性。
- **✅ 友好的中文日志**: 所有日志输出均已中文化，方便中文开发者理解程序的运行状态和结果。

##如何运行

### 1. 先决条件

- 已安装 **Java** (推荐 JDK 11 或更高版本)
- 已安装 **Maven**
- 已安装并启动 **Apache Flink** 集群 (或在 IDE 中直接运行)
- 已安装 `netcat` (或 `nc`) 工具用于模拟数据源

### 2. 打包项目

在项目根目录下，使用 Maven 进行打包：
```bash
mvn clean package
```
这将在 `target/` 目录下生成一个 JAR 文件，例如 `flink-wordcount-demo-1.0.jar`。

### 3. 启动数据源

打开一个新的终端窗口，启动一个 Socket 服务器，监听 `9999` 端口：
```bash
nc -lk 9999
```
此时，该终端会阻塞并等待客户端连接和数据输入。

### 4. 提交 Flink 作业

打开另一个终端窗口，使用 `flink run` 命令提交作业。

**使用默认参数 (`localhost:9999`)**
```bash
flink run -c cn.liboshuai.learn.flink.example.WordCount target/your-project-name.jar
```
> **注意**: 请将 `cn.liboshuai.learn.flink.example.WordCount` 替换为你的主类全路径名，`your-project-name.jar` 替换为实际的 JAR 文件名。

**指定自定义参数**
```bash
flink run -c cn.liboshuai.learn.flink.example.WordCount target/your-project-name.jar --hostname 192.168.1.100 --port 12345
```

### 5. 输入数据并查看结果

- **输入数据**: 回到运行 `nc` 的终端窗口，输入任意以空格分隔的单词，然后按回车。
  ```
  hello flink
  hello world
  flink is great
  ```

- **查看结果**: 在 Flink TaskManager 的 `.log` 文件中（或 IDE 的控制台），你将看到实时的计算结果。

## 预期输出示例

```log
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (hello,1)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (flink,1)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (hello,2)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (world,1)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (flink,2)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (is,1)
INFO  c.l.d.FlinkWordCountDemoOptimized - Flink 计算结果: (great,1)
```
> 每一行输出代表了当前单词的最新计数值。例如，当第二个 `hello` 到达时，`(hello,2)` 会被输出。