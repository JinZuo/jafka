package com.sohu.jafka.log.index;

import com.sohu.jafka.message.MessageId;

/**
 * LogIndex contains a message id and the offset of the message in the corresponding jafka file.
 * @author:rockybean(smilingrockybean@gmail.com)
 */
public class LogIndex {

    //the message id
    private long msgId;
    //the offset of the message in the corresponding jafka file
    private long offset;

    public LogIndex(long msgId,long offset){
        this.msgId = msgId;
        this.offset = offset;
    }

    public MessageId getMessageId() {
        return new MessageId(msgId);
    }

    public long getOffset() {
        return offset;
    }

    public long getMessageIdLongValue(){
        return msgId;
    }

    @Override
    public String toString() {
        return "LogIndex{" +
                "msgId=" + getMessageId() +
                ", offset=" + offset +
                '}';
    }
}
