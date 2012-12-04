package com.sohu.jafka.message;

import org.apache.log4j.Logger;


/**
 * To generate message id
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class MessageIdCenter {
    private static Logger logger = Logger.getLogger(MessageIdCenter.class);

    private static long lastTimestamp = -1L;
    private static int sequenceNum = 0;

    /**
         * Generate a message id by partitionId which contains timstamp(42bits),partitionId(10bits) and sequenceId(12bits).
         * @param partitionId
         * @return
         */
    public static synchronized long generateId(int partitionId) {
            if(partitionId < 0)
                return -1;
            long timestamp = System.currentTimeMillis();
            if(timestamp < lastTimestamp){
                throw new RuntimeException("Clock move backwards!Refused to generate id,please check your system config!");
            }
            if(timestamp == lastTimestamp){
                sequenceNum = (sequenceNum + 1) & MessageId.SEQUENCE_MASK;
                if(sequenceNum == 0){
                    timestamp = getNextMilli();
                }
            }else{
                sequenceNum = 0;
            }
            lastTimestamp = timestamp;

            if(logger.isDebugEnabled()){
                logger.debug(String.format("Generate new message id using {timestamp,partition,sequence} => {%d,%d,%d}",timestamp,partitionId,sequenceNum));
            }

            return timestamp << MessageId.TIMESTAMP_SHIFT|
                    partitionId << MessageId.PARTITIONID_SHIFT |
                    sequenceNum;
    }

    private static long getNextMilli() {
        if(logger.isDebugEnabled()){
            logger.debug("wait until next millisecond comes!");
        }
        long curTime = System.currentTimeMillis();
        while(curTime ==  lastTimestamp){
            curTime = System.currentTimeMillis();
        }
        return curTime;
    }
}
