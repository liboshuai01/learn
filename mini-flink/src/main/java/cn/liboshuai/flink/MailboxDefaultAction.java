package cn.liboshuai.flink;

public interface MailboxDefaultAction {

    /**
     * 执行默认动作。
     *
     * @param controller 用于与主循环交互（例如请求挂起）
     */
    void runDefaultAction(Controller controller) throws Exception;

    /**
     * 控制器：允许 DefaultAction 影响主循环的行为
     */
    interface Controller {
        /**
         * 告诉主循环：“我没活干了（Input 为空），请暂停调度我。”
         * 调用此方法后，主循环将不再调用 runDefaultAction，直到被 resume。
         */
        void suspendDefaultAction();
    }
}
