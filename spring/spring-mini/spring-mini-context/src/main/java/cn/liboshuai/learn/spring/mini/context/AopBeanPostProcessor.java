package cn.liboshuai.learn.spring.mini.context;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AopBeanPostProcessor implements BeanPostProcessor {

    private final List<Advisor> advisors = new ArrayList<>();

    public AopBeanPostProcessor(ApplicationContext applicationContext) {
        Map<String, BeanDefinition> beanDefinitionMap = applicationContext.getBeanDefinitionMap();
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            BeanDefinition beanDefinition = entry.getValue();
            Class<?> beanClass = beanDefinition.getBeanClass();
            if (!beanClass.isAnnotationPresent(Aspect.class)) {
                continue;
            }
            for (Method method : beanClass.getDeclaredMethods()) {
                method.setAccessible(true);
                // FIXME: 这里我们先只支持@Before注解，并且pointcut只支持从@Before中直接获取，不支持@Pointcut注解
                if (!method.isAnnotationPresent(Before.class)) {
                    continue;
                }
                String pointcut = method.getAnnotation(Before.class).value();
                Object aspect;
                try {
                    aspect = beanClass.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                         NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
                Advisor advisor = new Advisor(pointcut, method, aspect);
                advisors.add(advisor);
            }
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        for (Advisor advisor : advisors) {
            if (!advisor.pointcut.contains(bean.getClass().getName())) {
                continue;
            }
            // 1. 创建 Enhancer 类
            Enhancer enhancer = new Enhancer();
            // 2. 设置父类（目标类），CGLIB是通过继承来实现的
            enhancer.setSuperclass(bean.getClass());
            // 3. 设置回调（方法拦截器）
            enhancer.setCallback((MethodInterceptor) (o, method, objects, methodProxy) -> {
                // 通过反射执行Before中的增强方法
                advisor.method.invoke(advisor.aspect);
                return method.invoke(bean, objects);
            });
            // 4. 创建代理对象并返回
            return enhancer.create();
        }
        return bean;
    }

    /**
     * @param pointcut 切面规则
     * @param method   切面需要执行的方法
     * @param aspect   切面类对象
     */
    private record Advisor(String pointcut, Method method, Object aspect) {
    }
}
