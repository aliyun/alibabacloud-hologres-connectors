package com.alibaba.hologres.kafka.utils;

/** MessageInfo. */
public class MessageInfo {
    public Boolean wholeMessageInfo = false;
    public String messageTopic = "kafkatopic";
    public String messagePartition = "kafkapartition";
    public String messageOffset = "kafkaoffset";
    public String messageTimestamp = "kafkatimestamp";

    public MessageInfo(Boolean wholeMessageInfo) {
        this.wholeMessageInfo = wholeMessageInfo;
    }

    public void setMessageInfo(
            String messageTopic,
            String messagePartition,
            String messageOffset,
            String messageTimestamp) {
        this.messageTopic = messageTopic;
        this.messagePartition = messagePartition;
        this.messageOffset = messageOffset;
        this.messageTimestamp = messageTimestamp;
    }
}
