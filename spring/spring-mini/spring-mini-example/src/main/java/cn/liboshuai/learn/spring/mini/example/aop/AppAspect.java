package cn.liboshuai.learn.spring.mini.example.aop;

import cn.liboshuai.learn.spring.mini.context.Component;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
@Component
public class AppAspect {

    private static final Logger log = LoggerFactory.getLogger(AppAspect.class);

    @Before("execution(* cn.liboshuai.learn.spring.mini.example.service.OrderService.*(..))")
    public void beforeMethod() {
        log.info("调用了AppAspect类的beforeMethod方法");
    }
}
