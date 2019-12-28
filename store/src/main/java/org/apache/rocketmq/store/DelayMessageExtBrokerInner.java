package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExt;

public class DelayMessageExtBrokerInner extends MessageExt {

    private int deadlineSeconds;


    public int getDeadlineSeconds() {
        return deadlineSeconds;
    }

    public void setDeadlineSeconds(int deadlineSeconds) {
        this.deadlineSeconds = deadlineSeconds;
    }
}
