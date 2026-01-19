package com.liboshuai.spring.mini.context;

import java.util.Map;

public interface ApplicationContext {
    /**
     * 根据bean名称获取bean示例对象
     */
    Object getBean(String beanName);

    /**
     * 获取BeanDefinition的数量
     */
    int getBeanDefinitionCount();

    /**
     * 获取beanDefinitionMap
     */
    Map<String, BeanDefinition> getBeanDefinitionMap();
}
