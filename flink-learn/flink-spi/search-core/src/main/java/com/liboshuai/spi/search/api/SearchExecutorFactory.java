package com.liboshuai.spi.search.api;

public interface SearchExecutorFactory {
    String getName();

    boolean isCompatibleWith(String type);

    SearchExecutor getExecutor();
}
