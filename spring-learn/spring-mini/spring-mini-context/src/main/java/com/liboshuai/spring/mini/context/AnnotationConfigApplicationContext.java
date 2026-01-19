package com.liboshuai.spring.mini.context;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AnnotationConfigApplicationContext implements ApplicationContext {

    private static final Logger log = LoggerFactory.getLogger(AnnotationConfigApplicationContext.class);

    private final Map<String, BeanDefinition> beanDefinitionMap = new HashMap<>();

    private final Map<String, Object> singletonObjects = new ConcurrentHashMap<>(256);

    private final List<BeanPostProcessor> beanPostProcessors = new ArrayList<>();

    public static final String SCOPE_PROTOTYPE = "prototype";
    public static final String SCOPE_SINGLETON = "singleton";

    public AnnotationConfigApplicationContext(Class<?> configClazz) {
        // 1. 扫描@ComponentScan指定包路径下的所有bean，存放到beanDefinitionMap中 (BeanDefinition表示bean的定义信息）
        scanBeanDefinition(configClazz);
        // 2. 加载所有BeanPostProcessor，并存入beanPostProcessors中
        loadBeanPostProcessors();
        // 3. 创建所有单例非懒加载bean，存放到singletonObjects中
        loadSingletonObjects();
    }

    private void loadSingletonObjects() {
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            String beanName = entry.getKey();
            BeanDefinition beanDefinition = entry.getValue();
            if (beanDefinition.isLazy() || !SCOPE_SINGLETON.equals(beanDefinition.getScope())) {
                continue;
            }
            Object bean = createBean(beanDefinition);
            singletonObjects.put(beanName, bean);
        }
    }

    /**
     * 创建bean
     */
    private Object createBean(BeanDefinition beanDefinition) {
        Class<?> beanClass = beanDefinition.getBeanClass();
        String beanName = beanDefinition.getBeanName();
        // 实例化bean
        Object bean = newInstanceBean(beanClass);
        // 依赖注入
        di(bean);
        // 初始化bean
        invokeBeanNameAware(beanName, bean); // 执行BeanNameAware的setBeanName方法
        invokeApplicationContextAware(bean); // 执行ApplicationContextAware的setApplicationContext方法
        bean = invokeBeanPostProcessorBefore(beanName, bean); // 执行BeanPostProcessor的postProcessBeforeInitialization
        invokePostConstruct(bean); // 执行@PostConstruct方法
        invokeInitializingBean(bean); // 执行InitializingBean的afterPropertiesSet方法
        bean = invokeBeanPostProcessorAfter(beanName, bean); // 执行BeanPostProcessor的postProcessAfterInitialization方法
        return bean;
    }

    private static void invokeInitializingBean(Object bean) {
        if (bean instanceof InitializingBean initializingBean) {
            initializingBean.afterPropertiesSet();
        }
    }

    private static void invokePostConstruct(Object bean) {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (!method.isAnnotationPresent(PostConstruct.class)) {
                continue;
            }
            try {
                Object ignore = method.invoke(bean);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Object invokeBeanPostProcessorAfter(String beanName, Object bean) {
        for (BeanPostProcessor beanPostProcessor : beanPostProcessors) {
            bean = beanPostProcessor.postProcessAfterInitialization(bean, beanName);
        }
        return bean;
    }

    private Object invokeBeanPostProcessorBefore(String beanName, Object bean) {
        for (BeanPostProcessor beanPostProcessor : beanPostProcessors) {
            bean = beanPostProcessor.postProcessBeforeInitialization(bean, beanName);
        }
        return bean;
    }

    private void invokeApplicationContextAware(Object bean) {
        if (bean instanceof ApplicationContextAware applicationContextAware) {
            applicationContextAware.setApplicationContext(this);
        }
    }

    private void invokeBeanNameAware(String beanName, Object bean) {
        if (bean instanceof BeanNameAware beanNameAware) {
            beanNameAware.setBeanName(beanName);
        }
    }

    private void di(Object bean) {
        Class<?> beanClass = bean.getClass();
        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if (!field.isAnnotationPresent(Autowired.class)) {
                continue;
            }
            // FIXME: @Autowired其实是先按照类型查找，然后再按照名称查找，这里我们先只实现按照名称查找
            String fieldName = field.getName();
            if (!beanDefinitionMap.containsKey(fieldName)) {
                throw new IllegalStateException(beanClass + "中注入依赖bean[" + fieldName + "]时未找到bean");
            }
            // FIXME: 这里存在循环依赖问题，待引入三级缓存来解决
            Object diBean = getBean(fieldName);
            try {
                field.set(bean, diBean);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Object newInstanceBean(Class<?> beanClass) {
        Object bean;
        try {
            bean = beanClass.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        return bean;
    }

    private void loadBeanPostProcessors() {
        beanPostProcessors.add(new AopBeanPostProcessor(this));
        for (Map.Entry<String, BeanDefinition> entry : beanDefinitionMap.entrySet()) {
            BeanDefinition beanDefinition = entry.getValue();
            Class<?> beanClass = beanDefinition.getBeanClass();
            if (!BeanPostProcessor.class.isAssignableFrom(beanClass)) {
                continue;
            }
            BeanPostProcessor beanPostProcessor = (BeanPostProcessor) newInstanceBean(beanDefinition.getBeanClass());
            beanPostProcessors.add(beanPostProcessor);
        }
    }

    private void scanBeanDefinition(Class<?> configClazz) {
        // 效验一个传入的配置类
        verifyConfig(configClazz);
        // 获取扫描路径下所有类全限定名
        List<String> allClassNameList = getAllClassNameList(configClazz);
        // 将符合要求的类转为BeanDefinition，并存入beanDefinitionMap中
        for (String classNam : allClassNameList) {
            Class<?> aClass;
            try {
                aClass = Class.forName(classNam);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            // 一定要有@Component注解，不然不识别为一个bean
            if (!aClass.isAnnotationPresent(Component.class)) {
                continue;
            }
            // 获取beanName
            String beanName = aClass.getAnnotation(Component.class).value();
            if (beanName == null || beanName.trim().isEmpty()) {
                beanName = Introspector.decapitalize(aClass.getSimpleName());
            }
            // 获取scope
            String scope = SCOPE_SINGLETON;
            if (aClass.isAnnotationPresent(Scope.class)) {
                scope = aClass.getAnnotation(Scope.class).value();
            }
            // 获取lazy
            boolean lazy = false;
            if (aClass.isAnnotationPresent(Lazy.class)) {
                lazy = aClass.getAnnotation(Lazy.class).value();
            }
            // 创建 beanDefinition
            BeanDefinition beanDefinition = new BeanDefinition(beanName, aClass, scope, lazy);
            // 存入 beanDefinitionMap
            beanDefinitionMap.put(beanName, beanDefinition);
        }
    }

    private List<String> getAllClassNameList(Class<?> configClazz) {
        String[] componentScanValues = configClazz.getAnnotation(ComponentScan.class).value();
        List<String> allClassNameList = new ArrayList<>();
        for (String packageName : componentScanValues) {
            String packagePath = packageName.replace(".", "/");
            ClassLoader appClassLoader = this.getClass().getClassLoader();
            URL url = appClassLoader.getResource(packagePath);
            if (url == null) {
                throw new IllegalArgumentException("@ComponentScan注解的value值[{" + packageName + "}]非法");
            }
            List<String> classNameList = scanDirectory(new File(url.getFile()), packageName);
            allClassNameList.addAll(classNameList);
        }
        return allClassNameList;
    }

    /**
     * 递归地扫描一个目录，找到所有的 .class 文件，并将它们转换为完全限定类名。
     *
     * @param directory   开始扫描的目录。
     * @param basePackage 与该目录对应的基础包名 (例如 "com.liboshuai.demo")。
     * @return 一个包含完全限定类名的列表 (例如 "com.liboshuai.demo.service.MyService")。
     */
    private List<String> scanDirectory(File directory, String basePackage) {
        List<String> classNames = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files == null) {
            log.warn("要扫描的目录为空或不是一个有效目录: {}", directory.getPath());
            return classNames; // 返回空列表
        }

        for (File file : files) {
            if (file.isDirectory()) {
                // 如果是目录，就以更新后的包名递归扫描它
                classNames.addAll(scanDirectory(file, basePackage + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                // 如果是 .class 文件，就构建它的完全限定类名
                String simpleClassName = file.getName().substring(0, file.getName().length() - 6);
                classNames.add(basePackage + "." + simpleClassName);
            }
        }
        return classNames;
    }

    private static void verifyConfig(Class<?> configClazz) {
        if (!configClazz.isAnnotationPresent(Configuration.class)) {
            throw new IllegalStateException(configClazz + "不是一个配置类，请使用@Configuration注解标注");
        }
        if (!configClazz.isAnnotationPresent(ComponentScan.class)) {
            throw new IllegalStateException(configClazz + "请使用@ComponentScan注解标注");
        }
        String[] componentScanValues = configClazz.getAnnotation(ComponentScan.class).value();
        if (componentScanValues == null || componentScanValues.length == 0) {
            throw new IllegalStateException(configClazz + "的@ComponentScan注解请传入对应值");
        }
    }

    @Override
    public Object getBean(String beanName) {
        /*
            1. 单例非懒加载的bean直接从singletonObjects中get即可
            2. 单例懒加载的bean需要现在创建，并放入到singletonObjects中
            3. 多例bean每get一次就创建一次，且不需要放到singletonObjects中
         */
        BeanDefinition beanDefinition = beanDefinitionMap.get(beanName);
        if (beanDefinition == null) {
            throw new IllegalStateException("名称为[" + beanName + "]的bean不存在");
        }
        String scope = beanDefinition.getScope();
        boolean lazy = beanDefinition.isLazy();
        Object bean;
        if (SCOPE_PROTOTYPE.equals(scope)) { // 多例情况下，忽略lazy值
            bean = createBean(beanDefinition);
        } else { // 单例
            if (lazy) { // 单例懒加载
                // 使用ConcurrentHashMap的computeIfAbsent方法[检查-创建-存入]操作是原子的，保证线程安全
                bean = singletonObjects.computeIfAbsent(beanName, key -> createBean(beanDefinition));
            } else { // 单例非懒加载
                bean = singletonObjects.get(beanName);
            }
        }
        return bean;
    }

    @Override
    public int getBeanDefinitionCount() {
        return beanDefinitionMap.size();
    }

    @Override
    public Map<String, BeanDefinition> getBeanDefinitionMap() {
        return beanDefinitionMap;
    }
}
