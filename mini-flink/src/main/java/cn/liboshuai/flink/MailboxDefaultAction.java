package cn.liboshuai.flink;

public interface MailboxDefaultAction {
    void runDefaultAction(Controller controller) throws Exception;

    interface Controller {
        void suspendDefaultAction();
    }
}
