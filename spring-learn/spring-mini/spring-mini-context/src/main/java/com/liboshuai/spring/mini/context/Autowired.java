package com.liboshuai.spring.mini.context;

import java.lang.annotation.*;

@Documented // JavaDoc中展示
@Target(ElementType.FIELD) // 可以作用于字段上
@Retention(RetentionPolicy.RUNTIME) // 保留到运行时
public @interface Autowired {
}
