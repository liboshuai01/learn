package cn.liboshuai.demo.juc.threadpool;

public class Test {

    private volatile boolean isRunning = false;

    /**
     * 多个线程同时调用start()方法
     */
    public void start() {
        isRunning = true;
    }

    /**
     * 多个线程同时调用stop()方法
     */
    public void stop() {
        isRunning = false;
    }
}
