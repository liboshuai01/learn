package cn.liboshuai.flink;

public interface MailboxExecutor {

    /**
     * 提交一个任务到邮箱。
     *
     * @param command     业务逻辑
     * @param description 调试描述
     */
    void execute(ThrowingRunnable<? extends Exception> command, String description);
}
