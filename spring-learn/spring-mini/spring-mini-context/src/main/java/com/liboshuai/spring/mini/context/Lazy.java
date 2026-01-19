package com.liboshuai.spring.mini.context;

import java.lang.annotation.*;

@Documented // JavaDoc中展示
@Target(ElementType.TYPE) // 可以作用与接口、类等上
@Retention(RetentionPolicy.RUNTIME) //
public @interface Lazy {
    boolean value() default true;
}
