package cn.liboshuai.learn.spring.example.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy
@Component // 标记此类为bean
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);

    public void test() {
        log.info("调用了UserService的test方法");
    }
}
