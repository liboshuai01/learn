package com.liboshuai.spi.search;

import com.liboshuai.spi.search.api.SearchExecutor;
import com.liboshuai.spi.search.api.SearchExecutorFactory;
import com.liboshuai.spi.search.api.SearchExecutorServiceLoader;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        testDatabase();

        System.out.println("\n======================================================\n");

        testFile();

        System.out.println("\n======================================================\n");

        testEs();
    }

    private static void testDatabase() {
        SearchExecutorFactory searchExecutorFactory = SearchExecutorServiceLoader.getExecutorFactory("database");
        SearchExecutor searchExecutor = searchExecutorFactory.getExecutor();
        List<String> results = searchExecutor.executor("短视频平台");
        System.out.println("数据库搜索客户端执行的结果：" + results);
    }

    private static void testFile() {
        SearchExecutorFactory searchExecutorFactory = SearchExecutorServiceLoader.getExecutorFactory("file");
        SearchExecutor searchExecutor = searchExecutorFactory.getExecutor();
        List<String> results = searchExecutor.executor("项目文件");
        System.out.println("文件搜索客户端执行的结果：" + results);
    }

    private static void testEs() {
        try {
            SearchExecutorFactory searchExecutorFactory = SearchExecutorServiceLoader.getExecutorFactory("es");
            SearchExecutor searchExecutor = searchExecutorFactory.getExecutor();
            List<String> results = searchExecutor.executor("日志");
            System.out.println("es搜索客户端执行的结果：" + results);
        } catch (IllegalStateException e) {
            System.err.println("项目中没有引入基于es搜索的实现类, 所以会报错: " + e.getMessage());
        }
    }
}
