package com.sohu.jafka.message;

/**
 * MessageId contains timestamp partitionId and sequenceId.
 *
 * |timestamp(42bits)|partitionid(10bits)|sequenceid(12bits)
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class MessageId {

    public static final int SEQUENCE_MASK = 0xfff;
    public static final int SEQUENCE_BITS = 12;
    public static final int SEQUENCE_SHIFT = 0;

    public static final int PARTITIONID_MASK = 0x3ff;
    public static final int PARTITIONID_BITS = 10;
    public static final int PARTITIONID_SHIFT = SEQUENCE_SHIFT + SEQUENCE_BITS;

    public static final long TIMESTAMP_MASK = 0x3ffffffffffL;
    public static final int TIMESTAMP_SHIFT = PARTITIONID_SHIFT + PARTITIONID_BITS;

    private long messageId = -1L;
    private long timestamp = -1L;
    private int partitionId = -1;
    private int sequenceId = -1;

    public MessageId(long id){
        if(id == -1L){
            return;
        }
        messageId = id;
        sequenceId = (int)(id & SEQUENCE_MASK);
        partitionId = (int)((id >>> PARTITIONID_SHIFT) & PARTITIONID_MASK);
        timestamp = (id >>> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getLongValue() {
        return messageId;
    }

    @Override
    public String toString() {
        return "MessageId{" +
                "partitionId=" + partitionId +
                ", timestamp=" + timestamp +
                ", sequenceId=" + sequenceId +
                '}';
    }
}
