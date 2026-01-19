实现与同父工厂`spring-example`相同的日志打印效果，包含IOC、DI、AOP等等spring框架功能。

在启动类中添加vm参数:`--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED`，不然会报错。