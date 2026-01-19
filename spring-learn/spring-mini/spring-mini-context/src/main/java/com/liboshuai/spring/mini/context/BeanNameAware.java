package com.liboshuai.spring.mini.context;

public interface BeanNameAware extends Aware {
    void setBeanName(String name);
}
