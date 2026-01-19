package cn.liboshuai.learn.spring.mini.example;

import cn.liboshuai.learn.spring.mini.context.AnnotationConfigApplicationContext;
import cn.liboshuai.learn.spring.mini.example.config.AppConfig;
import cn.liboshuai.learn.spring.mini.example.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
    public static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        // 基于AppConfig配置类，初始化Spring注解驱动的applicationContext
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        OrderService orderService = (OrderService) context.getBean("OrderService");
        orderService.test();
    }
}
/*
调用了BeanNameAware的setBeanName方法, beanName: OrderService
调用了ApplicationContextAware的setApplicationContext方法, beanDefinitionCount: 11
调用了AppBeanPostProcessor的postProcessBeforeInitialization，beanName: OrderService
调用了OrderService的postConstructMethod方法
调用了InitializingBean的afterPropertiesSet方法
调用了AppBeanPostProcessor的postProcessAfterInitialization，beanName: OrderService
调用了AppAspect类的beforeMethod方法
调用了OrderService的test方法
调用了UserService的test方法
 */
