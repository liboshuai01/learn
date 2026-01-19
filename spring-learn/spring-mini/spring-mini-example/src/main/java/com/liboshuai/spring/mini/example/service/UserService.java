package com.liboshuai.spring.mini.example.service;

import com.liboshuai.spring.mini.context.Component;
import com.liboshuai.spring.mini.context.Lazy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Lazy
@Component // 标记此类为bean
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);

    public void test() {
        log.info("调用了UserService的test方法");
    }
}
