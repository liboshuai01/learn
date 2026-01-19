package cn.liboshuai.demo.loader.demo2;

import java.io.*;

/**
 * 演示如何自定义类加载器来打破双亲委派模型 (Child-First)
 */
public class BreakDelegationDemo {

    /**
     * 自定义类加载器
     */
    static class CustomClassLoader extends ClassLoader {

        // 定义我们自己的类加载路径 (模拟插件包)
        private final String customClassPath;

        public CustomClassLoader(String customClassPath, ClassLoader parent) {
            // 明确传递父加载器 (AppClassLoader)
            super(parent);
            this.customClassPath = customClassPath;
        }

        /**
         * 重写 findClass 方法，这是真正加载 .class 字节码的地方
         */
        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            System.out.println("  [CustomClassLoader]: 正在调用 findClass() 加载 " + name);
            byte[] classData = loadClassData(name);
            if (classData == null) {
                System.out.println("  [CustomClassLoader]: findClass() 未能在 " + customClassPath + " 中找到 " + name);
                throw new ClassNotFoundException(name);
            }

            // defineClass 将字节码转换为 Class 对象
            System.out.println("  [CustomClassLoader]: findClass() 成功在 " + customClassPath + " 中找到 " + name);
            return defineClass(name, classData, 0, classData.length);
        }

        /**
         * 重写 loadClass 方法，这是打破双亲委派的关键
         */
        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            System.out.println("\n[CustomClassLoader]: 收到加载请求: " + name);

            // 1. [关键：线程安全]
            //    我们必须像父类 ClassLoader.loadClass 一样，
            //    使用 getClassLoadingLock 来确保 "检查-加载" 这个复合操作的原子性
            synchronized (getClassLoadingLock(name)) {

                // 2. 检查类是否已经被加载过了
                //    这是 JVM 必需的，防止同一个类被加载多次
                Class<?> loadedClass = findLoadedClass(name);
                if (loadedClass != null) {
                    System.out.println("  [CustomClassLoader]: " + name + " 已经被加载过了 (来自 JVM 缓存)");
                    return loadedClass;
                }

                // 3. [关键：打破规则]
                //    我们只对我们自己的包 (com.example.*) 实行 "子优先 (Child-First)"
                if (name.startsWith("com.example.")) {
                    System.out.println("  [CustomClassLoader]: 这是一个 'com.example' 包下的类，执行 Child-First 策略");
                    try {
                        // 3a. [子优先] 优先尝试用 findClass() (即我们自己的路径) 加载
                        loadedClass = findClass(name);
                        if (loadedClass != null) {
                            // 成功找到，直接返回，加载流程结束
                            return loadedClass;
                        }
                    } catch (ClassNotFoundException e) {
                        // 在子加载器中没找到，没关系，继续往下
                        System.out.println("  [CustomClassLoader]: Child-First 未在 " + customClassPath + " 找到, 尝试委托给 Parent...");
                        // 注意：我们捕获了异常，并允许代码继续执行第 4 步
                    }
                } else {
                    System.out.println("  [CustomClassLoader]: 这*不是* 'com.example' 包下的类，执行 Parent-First (默认) 策略");
                }

                // 4. [遵守规则 / 回退]
                //    - 如果是 "非 com.example." 的类 (Parent-First)
                //    - 或者是 "com.example." 但在第 3 步的 findClass 中没找到 (回退到 Parent)
                //    都委托给父加载器 (AppClassLoader) 去走标准的双亲委派流程
                System.out.println("  [CustomClassLoader]: 委托给父加载器 (AppClassLoader) 加载 " + name);

                // 我们调用 super.loadClass()，它会安全地处理父类委托 和
                // 最终回退到调用 this.findClass() 的逻辑。
                // (虽然对于 "com.example." 的类，findClass 会被调用两次，但结果是正确的)
                return super.loadClass(name);
            }
        }

        /**
         * 辅助方法：从文件系统读取 .class 文件的字节码
         * (此方法无需修改)
         */
        private byte[] loadClassData(String className) { //
            // ... (省略未修改的代码)
            String path = customClassPath + File.separator + className.replace('.', File.separatorChar) + ".class";

            try (InputStream in = new FileInputStream(path);
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {

                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
                return out.toByteArray();

            } catch (IOException e) {
                return null;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("[Main]: AppClassLoader 的搜索路径 (java.class.path):\n" +
                System.getProperty("java.class.path"));

        // 1. 定义我们的 "插件" 目录 (`com/example/MyTestClass.java`类文件存放的目录, 一定要在上面`java.class.path`输出的目录列表之外)
        String customPath = "C:/Users/lbs/me/project/java-project/lbs-demo/java-base/temp_classes";

        // 2. 创建我们的自定义加载器
        //    它的父加载器是默认的 AppClassLoader
        CustomClassLoader loader = new CustomClassLoader(customPath,
                BreakDelegationDemo.class.getClassLoader());

        System.out.println("--- 尝试加载 'com.example.MyTestClass' (执行 Child-First) ---");
        // 3. 使用自定义加载器加载 "插件类"
        //    这将触发 CustomClassLoader.loadClass() -> findClass()
        Class<?> myClass = loader.loadClass("com.example.MyTestClass");
        Object myInstance = myClass.getDeclaredConstructor().newInstance();
        myClass.getMethod("sayHello").invoke(myInstance);

        System.out.println("\n\n--- 尝试加载 'java.lang.String' (执行 Parent-First) ---");
        // 4. 尝试加载一个 JRE 核心类
        //    这将触发 CustomClassLoader.loadClass() -> super.loadClass() -> ... -> Bootstrap
        Class<?> stringClass = loader.loadClass("java.lang.String");
        System.out.println("[Main]: 加载 String 成功，实际加载器: " + stringClass.getClassLoader() + " (null=Bootstrap)");

        System.out.println("\n\n--- 证明 AppClassLoader 无法加载 MyTestClass ---");
        // 5. 证明 AppClassLoader 确实找不到这个类 (因为它不在主类路径上)
        try {
            Class.forName("com.example.MyTestClass");
        } catch (ClassNotFoundException e) {
            System.out.println("[Main]: AppClassLoader 无法找到 'com.example.MyTestClass'，抛出异常: " + e.getClass().getName());
        }
    }
}
