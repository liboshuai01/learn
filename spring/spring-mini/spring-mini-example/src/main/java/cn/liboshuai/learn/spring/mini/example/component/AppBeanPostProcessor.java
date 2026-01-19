package cn.liboshuai.learn.spring.mini.example.component;

import cn.liboshuai.learn.spring.mini.context.BeanPostProcessor;
import cn.liboshuai.learn.spring.mini.context.Component;
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















