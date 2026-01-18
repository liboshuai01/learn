package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务基类。
 * 修改点：
 * 1. 增加了 ProcessingTimeService 的初始化和关闭。
 * 2. 提供了 getProcessingTimeService() 供子类使用。
 */
@Slf4j
public abstract class StreamTask implements MailboxDefaultAction {

    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mainMailboxExecutor;

    // [新增] 定时器服务
    protected final ProcessingTimeService timerService;

    public StreamTask() {
        Thread currentThread = Thread.currentThread();
        this.mailbox = new TaskMailboxImpl(currentThread);
        this.mailboxProcessor = new MailboxProcessor(this, mailbox);
        this.mainMailboxExecutor = mailboxProcessor.getMainExecutor();

        // [新增] 初始化定时器服务
        this.timerService = new SystemProcessingTimeService();
    }

    public final void invoke() throws Exception {
        log.info("[StreamTask] 任务已启动。");
        try {
            mailboxProcessor.runMailboxLoop();
        } catch (Exception e) {
            log.error("[StreamTask] 异常：{}", e.getMessage());
            throw e;
        } finally {
            close();
        }
    }

    private void close() {
        log.info("[StreamTask] 结束。");
        // [新增] 关闭定时器服务资源
        if (timerService != null) {
            timerService.shutdownService();
        }
        mailbox.close();
    }

    public MailboxExecutor getControlMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, MailboxProcessor.MIN_PRIORITY);
    }

    // [新增] 暴露给子类使用
    public ProcessingTimeService getProcessingTimeService() {
        return timerService;
    }

    @Override
    public abstract void runDefaultAction(Controller controller) throws Exception;

    public abstract void performCheckpoint(long checkpointId);
}
