package com.sohu.jafka.log.index;

import com.sohu.jafka.log.LogSegment;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.CompressionCodec;
import com.sohu.jafka.message.CompressionUtils;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.message.MessageId;
import com.sohu.jafka.message.MessageSet;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

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
        this(file,channel,false,logSegment,needRecovery);
    }

    public LogIndexSegment(File file, FileChannel channel, boolean mutable,LogSegment logSegment, boolean needRecovery){
        this.idxFile = file;
        this.channel = channel;
        this.logSegment = logSegment;
        this.mutable = mutable;

        if(needRecovery){
            try {
                recover();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
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

    //recover index file
    public void recover() throws IOException {
        checkMutable();
        Iterator<MessageAndOffset> itr = logSegment.getMessageSet().iterator();

        long len = channel.size()/16*16;
        if(len < channel.size()){
            channel.truncate(len);
        }

        long validUpTo = 0;
        long next = 0;
        ByteBuffer idxBuffer = ByteBuffer.allocate(16);

        while (itr.hasNext()){
            MessageAndOffset mas = itr.next();
            next = validateMessageIndex(mas,validUpTo,len,idxBuffer,-1);
            if(next >= 0){
                validUpTo = next;
            }else{
                len = validUpTo;
                channel.truncate(len);
                //try to recreate the message index
                next = validateMessageIndex(mas,validUpTo,len,idxBuffer,-1);
                if(next >= 0){
                    validUpTo = next;
                }else{
                    throw new IllegalStateException("Cannot recover the message index for "+logSegment.getName());
                }
            }

        }
        channel.truncate(validUpTo);
        channel.position(validUpTo);
        logger.info("Recover message index successfully!");

    }

    private long validateMessageIndex(MessageAndOffset mas, long validUpTo, long len, ByteBuffer idxBuffer,long parentOffset) throws IOException {
        long msgId = mas.message.messageId();
        long fileOffset = parentOffset == -1?(mas.offset - mas.message.serializedSize()):parentOffset;

        //compressed message
        if(mas.message.compressionCodec() != CompressionCodec.NoCompressionCodec){
            long processOffset = validUpTo;
            ByteBufferMessageSet byteBufferMessageSet = CompressionUtils.decompress(mas.message);
            for(MessageAndOffset tmpMas : byteBufferMessageSet){
                processOffset = validateMessageIndex(tmpMas,processOffset,len,idxBuffer,fileOffset);
                if(processOffset == -1){
                    break;
                }
            }
            //after verify all the messages in this compressed message,return the valid index offset
            return processOffset;
        }


        if(validUpTo >= len){
            append(msgId,fileOffset);
            validUpTo += 16;
        }else{
            idxBuffer.rewind();
            channel.read(idxBuffer,validUpTo);
            idxBuffer.rewind();
            long idxMsgId = idxBuffer.getLong();
            long idxOffset = idxBuffer.getLong();
            if(idxMsgId == msgId && idxOffset == fileOffset){
                validUpTo += 16;
            }else{
                return -1;
            }
        }
        return validUpTo;
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


    public long getOffsetByTime(long time) throws IOException{
        long fileOffset = getFileOffsetByTime(time);
        return fileOffset + logSegment.start();
    }
    /**
     * get the first message offset equal or bigger than time
     * @param time
     * @return
     * @author rockybean
     */
    public long getFileOffsetByTime(long time) throws IOException {
        if(getIndexNum() == 0)
            return -1;
        int partitionId = new MessageId(startMsgId).getPartitionId();
        long expectedMsgId = MessageId.generateId(time,partitionId , 0);
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
                logger.debug("choose left:"+leftIdx);
                return leftIdx;
            }
            if((mid + 1) > getIndexNum()){
                logger.error("mid +1 >high!");
                return null;
            }

            LogIndex rightIdx = getLogIndexAt(mid + 1);
            if(leftIdx.getMessageIdLongValue() < expectedMsgId && expectedMsgId <= rightIdx.getMessageIdLongValue()){
                logger.debug("choose right:"+rightIdx);
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
