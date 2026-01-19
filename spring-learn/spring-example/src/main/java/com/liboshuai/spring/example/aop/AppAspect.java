package com.liboshuai.spring.example.aop;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class AppAspect {

    private static final Logger log = LoggerFactory.getLogger(AppAspect.class);

    @Before("execution(* com.liboshuai.spring.example.service.OrderService.*(..))")
    public void beforeMethod() {
        log.info("调用了AppAspect类的beforeMethod方法");
    }
}
