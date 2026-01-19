package com.liboshuai.spi.search.file;

import com.liboshuai.spi.search.api.SearchExecutor;

import java.util.Arrays;
import java.util.List;

public class FileSearchExecutor implements SearchExecutor {
    @Override
    public List<String> executor(String keyword) {
        System.out.println("文件搜索执行器接收到的关键字为：" + keyword);
        return Arrays.asList("README.md", "HelloWorld.java");
    }
}
