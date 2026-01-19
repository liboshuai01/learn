package cn.liboshuai.learn.java.spi.search.file;

import cn.liboshuai.learn.java.spi.search.api.SearchExecutor;
import cn.liboshuai.learn.java.spi.search.api.SearchExecutorFactory;

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
