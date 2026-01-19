package com.liboshuai.spring.mini.context;

public interface ApplicationContextAware extends Aware{
    void setApplicationContext(ApplicationContext applicationContext);
}
