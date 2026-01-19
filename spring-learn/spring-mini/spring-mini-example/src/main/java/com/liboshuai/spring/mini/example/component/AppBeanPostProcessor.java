package com.liboshuai.spring.mini.example.component;

import com.liboshuai.spring.mini.context.BeanPostProcessor;
import com.liboshuai.spring.mini.context.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class AppBeanPostProcessor implements BeanPostProcessor {

    public static final Logger LOGGER = LoggerFactory.getLogger(AppBeanPostProcessor.class);

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        if ("OrderService".equals(beanName)) {
            LOGGER.info("调用了AppBeanPostProcessor的postProcessBeforeInitialization，beanName: {}", beanName);
        }
        return BeanPostProcessor.super.postProcessBeforeInitialization(bean, beanName);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if ("OrderService".equals(beanName)) {
            LOGGER.info("调用了AppBeanPostProcessor的postProcessAfterInitialization，beanName: {}", beanName);
        }
        return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
    }
}















