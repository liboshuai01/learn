package com.liboshuai.spring.mini.context;

import java.lang.annotation.*;

@Documented // JavaDoc中展示
@Target(ElementType.TYPE) // 可以作用与接口、类等上
@Retention(RetentionPolicy.RUNTIME) // 保留到运行时
public @interface Component {
    String value() default "";
}
