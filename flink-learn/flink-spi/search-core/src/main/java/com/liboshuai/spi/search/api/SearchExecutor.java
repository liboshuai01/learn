package com.liboshuai.spi.search.api;

import java.util.List;

public interface SearchExecutor {
    List<String> executor(String keyword);
}
