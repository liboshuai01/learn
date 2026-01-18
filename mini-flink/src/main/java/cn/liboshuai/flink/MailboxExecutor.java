package cn.liboshuai.flink;

public interface MailboxExecutor {
    void execute(ThrowingRunnable<? extends Exception> command, String description);
}
