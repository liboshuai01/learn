package cn.liboshuai.learn.java.spi.search.api;

import java.util.List;

public interface SearchExecutor {
    List<String> executor(String keyword);
}
