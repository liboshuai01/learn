package cn.liboshuai.learn.java.spi.search.database;

import cn.liboshuai.learn.java.spi.search.api.SearchExecutor;
import cn.liboshuai.learn.java.spi.search.api.SearchExecutorFactory;

public class DatabaseSearchExecutorFactory implements SearchExecutorFactory {

    public static final String NAME = "database";

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
        return new DatabaseSearchExecutor();
    }
}
