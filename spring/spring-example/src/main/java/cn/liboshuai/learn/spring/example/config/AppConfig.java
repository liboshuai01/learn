package cn.liboshuai.learn.spring.example.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration // 声明该类为Spring的核心配置类，用于定义应用上下文
@ComponentScan("cn.liboshuai.learn.spring.example") // 启用组件扫描，自动发现并注册指定包下的bean
@EnableAspectJAutoProxy // 启用AspectJ的自动代理功能，从而支持基于注解的AOP
public class AppConfig {
}
