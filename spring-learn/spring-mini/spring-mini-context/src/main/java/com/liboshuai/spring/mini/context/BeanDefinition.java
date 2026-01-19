package com.liboshuai.spring.mini.context;

/**
 * Bean定义信息
 */
public class BeanDefinition {
    private final String beanName;
    private final Class<?> beanClass;
    private final String scope;
    private final boolean lazy;


    public BeanDefinition(String beanName, Class<?> beanClass, String scope, boolean lazy) {
        this.beanName = beanName;
        this.beanClass = beanClass;
        this.scope = scope;
        this.lazy = lazy;
    }

    public String getBeanName() {
        return beanName;
    }

    public Class<?> getBeanClass() {
        return beanClass;
    }

    public String getScope() {
        return scope;
    }

    public boolean isLazy() {
        return lazy;
    }
}
