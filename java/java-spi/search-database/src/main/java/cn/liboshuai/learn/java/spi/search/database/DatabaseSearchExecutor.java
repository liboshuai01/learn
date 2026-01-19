package cn.liboshuai.learn.java.spi.search.database;

import cn.liboshuai.learn.java.spi.search.api.SearchExecutor;

import java.util.Arrays;
import java.util.List;

public class DatabaseSearchExecutor implements SearchExecutor {
    @Override
    public List<String> executor(String keyword) {
        System.out.println("数据库搜索执行器接收到的关键字为：" + keyword);
        return Arrays.asList("抖音", "快手");
    }
}
