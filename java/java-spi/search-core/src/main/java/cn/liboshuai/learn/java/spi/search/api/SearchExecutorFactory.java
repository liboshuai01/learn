package cn.liboshuai.learn.java.spi.search.api;

public interface SearchExecutorFactory {
    String getName();

    boolean isCompatibleWith(String type);

    SearchExecutor getExecutor();
}
