package cn.liboshuai.flink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class StreamTask implements MailboxDefaultAction{
    protected final TaskMailbox mailbox;
    protected final MailboxProcessor mailboxProcessor;
    protected final MailboxExecutor mainMailboxExecutor;
    protected final ProcessingTimeService timerService;

    public StreamTask() {
        Thread currentThread = Thread.currentThread();
        this.mailbox = new TaskMailboxImpl(currentThread);
        this.mailboxProcessor = new MailboxProcessor(this, mailbox);
        this.mainMailboxExecutor = mailboxProcessor.getMainExecutor();
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
        log.info("[StreamTask] 结束");
        if (timerService != null) {
            timerService.shutdownService();
        }
        mailbox.close();
    }

    public MailboxExecutor getControlMailboxExecutor() {
        return new MailboxExecutorImpl(mailbox, MailboxProcessor.MIN_PRIORITY);
    }

    public abstract void performCheckpoint(long checkpointId);
}
