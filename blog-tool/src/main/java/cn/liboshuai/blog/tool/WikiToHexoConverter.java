package cn.liboshuai.blog.tool;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wiki.js Markdown 批量转换为 Hexo Markdown 工具
 * 环境要求: Java 17+
 * 使用方法: 修改 SOURCE_DIR 和 TARGET_DIR 后运行 main 方法
 */
public class WikiToHexoConverter {

    // TODO: 请修改为你本地的实际路径
    private static final String SOURCE_DIR = "/home/lbs/tmp/wiki";
    private static final String TARGET_DIR = "/home/lbs/tmp/hexo";

    // 默认封面图 (如果源文件没有封面，使用此图)
    private static final String DEFAULT_COVER = "https://lbs-images.oss-cn-shanghai.aliyuncs.com/default_cover.png";

    private static final DateTimeFormatter HEXO_DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Path sourcePath = Paths.get(SOURCE_DIR);
        Path targetPath = Paths.get(TARGET_DIR);

        if (!Files.exists(sourcePath)) {
            System.err.println("源目录不存在: " + sourcePath);
            return;
        }

        try {
            // 创建目标目录
            Files.createDirectories(targetPath);

            try (Stream<Path> paths = Files.walk(sourcePath)) {
                paths.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".md"))
                        .forEach(file -> convertFile(file, targetPath));
            }
            System.out.println("✅ 所有文件转换完成！");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void convertFile(Path sourceFile, Path targetDir) {
        try {
            String originalFilename = sourceFile.getFileName().toString().replace(".md", "");
            String content = Files.readString(sourceFile, StandardCharsets.UTF_8);

            // 1. 解析 YAML Front Matter 和 正文
            // Wiki.js 的 md 文件通常以 --- 开头
            String[] parts = content.split("---", 3);

            if (parts.length < 3) {
                System.out.println("⚠️ 跳过非标准格式文件: " + sourceFile.getFileName());
                return;
            }

            String yamlContent = parts[1];
            String bodyContent = parts[2];

            // 2. 解析元数据 Map
            Map<String, String> meta = parseSimpleYaml(yamlContent);

            // 3. 构建 Hexo 元数据
            StringBuilder newYaml = new StringBuilder();
            newYaml.append("---\n");

            // Title
            String title = meta.getOrDefault("title", "Untitled");
            newYaml.append("title: ").append(title).append("\n");

            // Abbrlink (使用原文件名)
            newYaml.append("abbrlink: ").append(originalFilename).append("\n");

            // Date (ISO 转 yyyy-MM-dd HH:mm:ss)
            String dateStr = meta.get("date"); // Wiki.js output format: 2025-08-01T21:52:12.670Z
            if (dateStr != null) {
                try {
                    Instant instant = Instant.parse(dateStr);
                    String hexoDate = HEXO_DATE_FMT.format(instant.atZone(ZoneId.systemDefault()));
                    newYaml.append("date: ").append(hexoDate).append("\n");
                } catch (Exception e) {
                    // 如果解析失败，保留原样或使用当前时间
                    newYaml.append("date: ").append(dateStr).append("\n");
                }
            }

            // Tags & Categories
            // Wiki.js 示例: tags: 大数据 (可能是逗号分隔字符串)
            String tagsRaw = meta.get("tags");
            List<String> tagsList = new ArrayList<>();
            if (tagsRaw != null && !tagsRaw.isBlank()) {
                // 简单处理逗号分隔
                tagsList = Arrays.stream(tagsRaw.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
            }

            if (!tagsList.isEmpty()) {
                newYaml.append("tags:\n");
                for (String tag : tagsList) {
                    newYaml.append("  - ").append(tag).append("\n");
                }

                // 逻辑: 默认取第一个 tag 作为 category，你可以根据需要修改此逻辑
                newYaml.append("categories:\n");
                newYaml.append("  - ").append(tagsList.get(0)).append("\n");
            }

            // Cover (Wiki.js 通常不带 cover，这里根据你的需求尝试获取或使用默认)
//            newYaml.append("cover: '").append(DEFAULT_COVER).append("'\n");

            // TOC
            newYaml.append("toc: true\n");

            newYaml.append("---");

            // 4. 组合新内容
            String newContent = newYaml.toString() + "\n" + bodyContent;

            // 5. 确定新文件名 (使用 Title，去除非法字符)
            String safeTitle = title.replaceAll("[\\\\/:*?\"<>|]", "_").trim(); // Windows 非法字符替换
            if (safeTitle.isEmpty()) {
                safeTitle = originalFilename;
            }
            Path targetFile = targetDir.resolve(safeTitle + ".md");

            // 6. 写入文件
            Files.writeString(targetFile, newContent, StandardCharsets.UTF_8);
            System.out.println("处理: " + originalFilename + " -> " + targetFile.getFileName());

        } catch (IOException e) {
            System.err.println("处理文件失败: " + sourceFile + " Error: " + e.getMessage());
        }
    }

    /**
     * 一个非常简易的 YAML 键值对解析器 (不依赖第三方库)
     * 仅适用于简单的 key: value 结构
     */
    private static Map<String, String> parseSimpleYaml(String yamlStr) {
        Map<String, String> result = new HashMap<>();
        String[] lines = yamlStr.split("\\r?\\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {continue;}

            int colonIndex = line.indexOf(':');
            if (colonIndex > 0) {
                String key = line.substring(0, colonIndex).trim();
                String value = line.substring(colonIndex + 1).trim();

                // 去除可能存在的引号
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }

                result.put(key, value);
            }
        }
        return result;
    }
}
