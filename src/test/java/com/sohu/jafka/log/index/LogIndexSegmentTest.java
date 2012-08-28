package com.sohu.jafka.log.index;

import static org.junit.Assert.*;
import com.sohu.jafka.message.MessageIdCenter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class LogIndexSegmentTest {

    private File idxFile;
    private LogIndexSegment idxSegment;

    public void createIndexSegments() throws FileNotFoundException {
        idxFile = new File("1.jafka.idx");
        FileChannel channel = new RandomAccessFile(idxFile,"rw").getChannel();
        idxSegment = new LogIndexSegment(idxFile,channel,null,false);
    }

    public void createLogIndexSegments(int num,int interval) throws IOException{
        System.out.println("produce "+num+" index data.....");
        long startTime = System.currentTimeMillis();
        for(int i = 0;i < num;i++){
            if(i%100 == 0)
                System.out.print(".");
            if(i!= 0 &&i%13000 == 0)
                System.out.println();
            long msgId = MessageIdCenter.generateId(3);
            idxSegment.append(msgId,i);
            if(interval > 0)
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
        }
        System.out.println();
        System.out.println("use time: "+(System.currentTimeMillis()-startTime)+"ms");
        System.out.println("file size is " + idxSegment.getSizeInBytes() + " B");
    }


    @Test
    public void testIndexSegment() throws IOException {
        createIndexSegments();
        createLogIndexSegments(new Random().nextInt(10000),0);
        //createLogIndexSegments(100,0);
        /*for(int i = 1;i <= idxSegment.getIndexNum();i++){
            LogIndex idx = idxSegment.getLogIndexAt(i);
            System.out.println(String.format("%d => msgId[%d],offset[%d]",i,idx.getMessageIdLongValue(),idx.getOffset()));
        }*/
        int idxNum = new Random().nextInt(idxSegment.getIndexNum())+1;
        //int idxNum = idxSegment.getIndexNum();
        LogIndex randomIdx = idxSegment.getLogIndexAt(idxNum);
        long time = randomIdx.getMessageId().getTimestamp();
        long startTime = System.currentTimeMillis();
        long offset = idxSegment.getOffsetByTime(time);
        System.out.println("use time:"+(System.currentTimeMillis()-startTime)+"ms");
        System.out.println(idxNum+"----->"+randomIdx+"??"+offset);
        //assertEquals(randomIdx.getOffset(), offset);
        System.out.println("sequenceId is "+randomIdx.getMessageId().getSequenceId());
        int firstIdxNum = idxNum - randomIdx.getMessageId().getSequenceId();
        LogIndex firstIdx = idxSegment.getLogIndexAt(firstIdxNum <= 0?1:firstIdxNum);
        assertEquals(firstIdx.getOffset(),offset);
        assertEquals(firstIdx.getMessageId().getTimestamp(), time);
        closeLogIndexSegments();
    }

    /*@Test
    public void testloop() throws IOException {
        for(int i=0;i<10000;i++){
            System.out.println(i+">***************************");
            testIndexSegment();
        }
    }*/

    @Test
    public void testIndexSegmentNotEqualTime() throws IOException {
        createIndexSegments();
        createLogIndexSegments(1000,2);
        int idxNum = new Random().nextInt(idxSegment.getIndexNum())+1;
        System.out.println("test #"+idxNum);
        LogIndex idx = idxSegment.getLogIndexAt(idxNum);
        LogIndex rightIdx = idxSegment.getLogIndexAt(idxNum+1);
        long getTime = idx.getMessageId().getTimestamp()+2;
        System.out.println("get time :"+getTime+",seqid is "+idx.getMessageId().getSequenceId());
        System.out.println("current time is "+idx.getMessageId().getTimestamp()+",current idx seq is "+idx.getMessageId().getSequenceId());
        System.out.println("right time is "+rightIdx.getMessageId().getTimestamp()+",right idx seq is "+rightIdx.getMessageId().getSequenceId());
        assertEquals(rightIdx!=null?rightIdx.getOffset():-1,idxSegment.getOffsetByTime(getTime));
    }


    @After
    public void closeLogIndexSegments() throws IOException {
        if(idxSegment != null){
            idxSegment.close();
        }
        if(idxFile.exists()){
            idxFile.delete();
        }
    }
}
