package com.liboshuai.spring.example.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

@Component
public class AppBeanPostProcessor implements BeanPostProcessor {

    public static final Logger LOGGER = LoggerFactory.getLogger(AppBeanPostProcessor.class);

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        if ("OrderService".equals(beanName)) {
            LOGGER.info("调用了AppBeanPostProcessor的postProcessBeforeInitialization，beanName: {}", beanName);
        }
        return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if ("OrderService".equals(beanName)) {
            LOGGER.info("调用了AppBeanPostProcessor的postProcessAfterInitialization，beanName: {}", beanName);
        }
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }
}















