package cn.liboshuai.flink;

import java.util.Optional;

public interface TaskMailbox {
    boolean hasMail();

    Optional<Mail> tryTake(int priority);

    Mail take(int priority) throws InterruptedException;

    void put(Mail mail);

    void close();

    enum State {
        OPEN,
        QUIESCED,
        CLOSED
    }

}
