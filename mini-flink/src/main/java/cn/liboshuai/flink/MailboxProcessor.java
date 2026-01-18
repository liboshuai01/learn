package cn.liboshuai.flink;

import lombok.Getter;

import java.lang.ref.PhantomReference;
import java.util.Optional;

public class MailboxProcessor implements MailboxDefaultAction.Controller{

    public static final int MIN_PRIORITY = 0;
    public static final int DEFAULT_PRIORITY = 1;

    private final MailboxDefaultAction defaultAction;
    private final TaskMailbox mailbox;

    @Getter
    private final MailboxExecutor mainExecutor;

    private boolean isDefaultActionAvailable = true;

    public MailboxProcessor(MailboxDefaultAction defaultAction, TaskMailbox mailbox) {
        this.defaultAction = defaultAction;
        this.mailbox = mailbox;
        this.mainExecutor = new MailboxExecutorImpl(mailbox, DEFAULT_PRIORITY);
    }

    public void runMailboxLoop() throws Exception {
        while (true) {
            while (processMail(mailbox, MIN_PRIORITY)) {

            }
            if (isDefaultActionAvailable) {
                defaultAction.runDefaultAction(this);
            } else {
                Mail mail = mailbox.take(DEFAULT_PRIORITY);
                mail.run();
            }
        }
    }

    private boolean processMail(TaskMailbox mailbox, int priority) throws Exception {
        Optional<Mail> mail = mailbox.tryTake(priority);
        if (mail.isPresent()) {
            mail.get().run();
            return true;
        }
        return false;
    }

    @Override
    public void suspendDefaultAction() {
        this.isDefaultActionAvailable = false;
    }

    public void resumeDefaultAction() {
        mailbox.put(new Mail(() -> this.isDefaultActionAvailable = true, MIN_PRIORITY, "Resume Default Action"));
    }

}
