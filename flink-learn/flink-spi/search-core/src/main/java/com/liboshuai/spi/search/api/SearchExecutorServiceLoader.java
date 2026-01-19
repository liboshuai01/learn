package com.liboshuai.spi.search.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class SearchExecutorServiceLoader {

    public static SearchExecutorFactory getExecutorFactory(String type) {
        if (type == null) {
            throw new NullPointerException();
        }

        final ServiceLoader<SearchExecutorFactory> loader =
                ServiceLoader.load(SearchExecutorFactory.class);

        final List<SearchExecutorFactory> compatibleFactories = new ArrayList<>();
        final Iterator<SearchExecutorFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                final SearchExecutorFactory factory = factories.next();
                if (factory != null && factory.isCompatibleWith(type)) {
                    compatibleFactories.add(factory);
                }
            } catch (Throwable e) {
                if (e.getCause() instanceof NoClassDefFoundError) {
                    System.out.println("无法加载工厂，缺少依赖项。");
                } else {
                    throw e;
                }
            }
        }
        if (compatibleFactories.size() > 1) {
            throw new IllegalStateException("发现多个兼容的客户端工厂: " + compatibleFactories);
        }

        if (compatibleFactories.isEmpty()) {
            throw new IllegalStateException("未找到 ExecutorFactory 来执行应用程序。");
        }

        return compatibleFactories.get(0);
    }
}
