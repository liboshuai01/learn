package cn.liboshuai.learn.spring.mini.context;

public interface BeanNameAware extends Aware {
    void setBeanName(String name);
}
