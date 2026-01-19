package com.liboshuai.spi.search.file;

import com.liboshuai.spi.search.api.SearchExecutor;
import com.liboshuai.spi.search.api.SearchExecutorFactory;

public class FileSearchExecutorFactory implements SearchExecutorFactory {

    public static final String NAME = "file";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isCompatibleWith(String type) {
        return NAME.equals(type);
    }

    @Override
    public SearchExecutor getExecutor() {
        return new FileSearchExecutor();
    }
}
