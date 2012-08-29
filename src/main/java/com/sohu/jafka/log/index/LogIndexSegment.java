package com.sohu.jafka.log.index;

import com.sohu.jafka.log.LogSegment;
import com.sohu.jafka.message.*;
import com.sohu.jafka.utils.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * the log indexes in a jafka.idx file
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class LogIndexSegment {

    private Logger logger = Logger.getLogger(LogIndexSegment.class);
    public static final String FILE_SUFFIX = ".idx";
    private static final int MESSAGEID_BYTES_NUM = 8;
    private static final int OFFSET_BYTES_NUM = 8;
    private static final int INDEX_BYTES_NUM = MESSAGEID_BYTES_NUM + OFFSET_BYTES_NUM;
    //the time of the first index in this file
    //private long startTime;
    private long startMsgId;
    //the time of the last index in this file
    //private long endTime;
    private long endMsgId;
    //the total index number
    private int indexNum;
    //the size of the file in bytes
    private long size;
    //the index file,which contains message index
    private File idxFile;
    private FileChannel channel;
    private LogSegment logSegment;
    private boolean mutable;

    public LogIndexSegment(File file, FileChannel channel, LogSegment logSegment, boolean needRecovery){
        this.idxFile = file;
        this.channel = channel;
        this.logSegment = logSegment;

        if(needRecovery){
            recover();
        }

        try {
            this.size = channel.size();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        this.indexNum = (int)size/INDEX_BYTES_NUM;
        try {
            loadStartAndEndId();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    //todo:alfred:recover mode
    //1.check the completeness of the index file:if its size can be divided evenly.
    //2.check the jafka file, and at the same time, check the offset of its idx file
    //3.if the idx file has errors, try to rebuild it.
    //      create when
    public boolean recover(){
        if(!recoverQuick()){
            //recover slowly
        }

        return false;
    }

    private boolean recoverQuick() {
        return false;
    }

    public void append(long messageId,long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(INDEX_BYTES_NUM);
        buffer.putLong(messageId);
        buffer.putLong(offset);
        buffer.rewind();
        channel.write(buffer);
        refreshData();
    }

    /**
     * refresh the index data after appending a new index
     * @throws IOException
     */
    private void refreshData() throws IOException {
        size = channel.size();
        indexNum = (int)size/INDEX_BYTES_NUM;
        if(indexNum > 0){
            if(startMsgId == -1){
                startMsgId = getLogIndexAt(1).getMessageIdLongValue();
            }
            endMsgId = getLogIndexAt(indexNum).getMessageIdLongValue();
        }
    }

    public LogIndex getLogIndexAt(int indexNum) throws IOException {
        if(this.indexNum <= 0 || indexNum <= 0 || indexNum > this.indexNum)
            return null;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,(indexNum-1)*INDEX_BYTES_NUM,INDEX_BYTES_NUM);
        return new LogIndex(buffer.getLong(),buffer.getLong());
    }

    /**
     * set start and end time
     */
    private void loadStartAndEndId() throws IOException {
        if(channel.size() == 0){
            startMsgId = -1L;
            endMsgId = -1L;
            return;
        }
        MessageId msgId = getLogIndexAt(1).getMessageId();
        startMsgId = getLogIndexAt(1).getMessageIdLongValue();
        endMsgId = getLogIndexAt(indexNum).getMessageIdLongValue();
        logger.info(String.format("Load startId (%d) and endId (%d) from index file [%s]",startMsgId,endMsgId,idxFile.getAbsolutePath()));
        /*try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,0,MESSAGEID_BYTES_NUM*8);
            startTime = buffer.getLong();
            buffer.clear();
            buffer = channel.map(FileChannel.MapMode.READ_ONLY,(size - 1)*INDEX_BYTES_NUM,MESSAGEID_BYTES_NUM*8);
            //buffer.flip();
            endTime = buffer.getLong();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/
    }


    /**
     * get the first message offset equal or bigger than time
     * @param time
     * @return
     */
    //todo:alfred:getIndexNum会不会受到多线程的影响？有人写有人查的时候？copyonwrite
    public long getOffsetByTime(long time) throws IOException {
        if(getIndexNum() == 0)
            return -1;
        int partitionId = new MessageId(startMsgId).getPartitionId();
        long expectedMsgId = MessageIdCenter.generateId(time,partitionId , 0);
        LogIndex expectedIdx = getLogIndexByMsgId(expectedMsgId);
        if(expectedIdx == null){
            return -1;
        }
        return expectedIdx.getOffset();
    }

    private LogIndex getLogIndexByMsgId(long expectedMsgId) throws IOException {
        if(getIndexNum() == 0)
            return null;
        if(expectedMsgId <= startMsgId){
            return getLogIndexAt(1);
        }
        int low = 1;
        int high = getIndexNum();
        int mid;

        while(low <= high){
            mid = (low + high)/2;
            LogIndex leftIdx = getLogIndexAt(mid);
            if(leftIdx.getMessageIdLongValue() == expectedMsgId){
                return leftIdx;
            }
            if((mid + 1) > getIndexNum()){
                logger.error("mid +1 >high!");
                return null;
            }

            LogIndex rightIdx = getLogIndexAt(mid + 1);
            if(leftIdx.getMessageIdLongValue() < expectedMsgId && expectedMsgId <= rightIdx.getMessageIdLongValue()){
                return rightIdx;
            }

            if(leftIdx.getMessageIdLongValue() > expectedMsgId){
                high = mid - 1;
            }else if(rightIdx.getMessageIdLongValue() < expectedMsgId){
                low = mid + 1;
            }
        }
        logger.warn("not found!");
        return null;
    }


    /**
     * just for test
     * @param time
     * @param length
     * @return
     * @throws IOException
     */
    public MessageSet getMessageSetByTime(long time, int length) throws IOException {
        long offset = getOffsetByTime(time);
        MessageSet messageSet = this.logSegment.getMessageSet().read(offset, length);
        //should return a byteBufferMessageSet
        return messageSet;
    }

    public void close() throws IOException {
        if(mutable)
            flush();
        channel.close();
    }

    public void flush() throws IOException {
        checkMutable();
        channel.force(true);
        //todo:alfred:add some statistics code
    }

    private void checkMutable(){
        if(!mutable)
            throw new IllegalStateException("try to modify an immutable index file!");
    }

    public File getIdxFile() {
        return idxFile;
    }

    public void setIdxFile(File idxFile) {
        this.idxFile = idxFile;
    }

    public long getStartTime() {
        return new MessageId(startMsgId).getTimestamp();
    }

    public long getEndTime() {
        return new MessageId(endMsgId).getTimestamp();
    }

    public int getIndexNum(){
        return this.indexNum;
    }

    public long getSizeInBytes(){
        return this.size;
    }

    public LogSegment getLogSegment(){
        return logSegment;
    }

    /**
     * compare a given time with the start and end time in this segment and returns :
     * @param value
     * @return
     */
    public int contains(long value){
        if(value < getStartTime())
            return -1;
        if(value > getEndTime())
            return 1;
        return 0;
    }
}
