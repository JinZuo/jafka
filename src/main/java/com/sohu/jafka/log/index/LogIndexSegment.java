package com.sohu.jafka.log.index;

import com.sohu.jafka.log.LogSegment;
import com.sohu.jafka.message.MessageId;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * the log indexes in a jafka.idx file
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class LogIndexSegment {

    public static final String FILE_SUFFIX = ".idx";
    private static final int MESSAGEID_BYTES_NUM = 8;
    private static final int OFFSET_BYTES_NUM = 8;
    private static final int INDEX_BYTES_NUM = MESSAGEID_BYTES_NUM + OFFSET_BYTES_NUM;
    //the time of the first index in this file
    private long startTime;
    //the time of the last index in this file
    private long endTime;
    //the total index number
    private int indexNum;
    //the size of the file in bytes
    private long size;
    //the index file,which contains message index
    private File idxFile;
    private FileChannel channel;

    private boolean mutable;

    public LogIndexSegment(File file,FileChannel channel){
        this.idxFile = file;
        this.channel = channel;
        try {
            this.size = channel.size();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        this.indexNum = (int)size/INDEX_BYTES_NUM;
        try {
            loadTime();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    //todo:alfred:recover mode
    //1.check the completeness of the index file:if its size can be divided evenly.
    //2.check the jafka file, and at the same time, check the offset of its idx file
    //3.if the idx file has errors, try to rebuild it.
    //      create when
    public void recover(){

    }

    public void append(long messageId,long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(64);
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
        this.size = channel.size();
        this.indexNum = (int)size/INDEX_BYTES_NUM;
        this.endTime = getLogIndexAt(indexNum).getMessageId().getTimestamp();
    }

    private LogIndex getLogIndexAt(int indexNum) throws IOException {
        if(channel.size() == 0)
            return null;
        LogIndex idx = new LogIndex();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,(indexNum-1)*INDEX_BYTES_NUM*8,64);
        //todo:alfred:is this necessary?
        buffer.rewind();
        idx.setMessageId(new MessageId(buffer.getLong()));
        idx.setOffset(buffer.getLong());
        return idx;
    }

    /**
     * set start and end time
     */
    private void loadTime() throws IOException {
        if(channel.size() == 0){
            return;
        }
        try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY,0,MESSAGEID_BYTES_NUM*8);
            //todo: alfred flip ??
            buffer.flip();
            startTime = buffer.getLong();
            buffer.clear();
            buffer = channel.map(FileChannel.MapMode.READ_ONLY,(size - 1)*INDEX_BYTES_NUM,MESSAGEID_BYTES_NUM*8);
            buffer.flip();
            endTime = buffer.getLong();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }


    /**
     * get the first message offset bigger than time
     * @param time
     * @return
     */
    public long getOffsetByTime(long time){
        try {
            FileInputStream fis = new FileInputStream(idxFile);
            FileChannel channel = fis.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY,0,channel.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return -1;
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
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    /**
     * compare a given time with the start and end time in this segment and returns :
     * @param value
     * @return
     */
    public int contains(long value){
        if(value < startTime)
            return -1;
        if(value > endTime)
            return 1;
        return 0;
    }
}
