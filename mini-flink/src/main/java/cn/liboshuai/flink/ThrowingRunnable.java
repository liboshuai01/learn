package cn.liboshuai.flink;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
}
