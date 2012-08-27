package com.sohu.jafka.log.index;

import com.sohu.jafka.message.MessageId;

/**
 * LogIndex contains a message id and the offset of the message in the corresponding jafka file.
 * @author:rockybean(smilingrockybean@gmail.com)
 */
public class LogIndex {

    //the message id
    private MessageId messageId;
    //the offset of the message in the corresponding jafka file
    private long offset;


    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
