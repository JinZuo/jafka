/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sohu.jafka.log;

import static java.lang.String.format;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.sohu.jafka.log.index.IndexSegmentList;
import com.sohu.jafka.log.index.LogIndexSegment;
import com.sohu.jafka.message.*;
import com.sohu.jafka.server.Server;
import org.apache.log4j.Logger;

import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.common.InvalidMessageSizeException;
import com.sohu.jafka.common.OffsetOutOfRangeException;
import com.sohu.jafka.mx.BrokerTopicStat;
import com.sohu.jafka.mx.LogStats;
import com.sohu.jafka.utils.KV;
import com.sohu.jafka.utils.Range;
import com.sohu.jafka.utils.Utils;

/**
 * a log is a message sets with more than one files.
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class Log implements ILog {

    private final Logger logger = Logger.getLogger(Log.class);

    private static final String FileSuffix = ".jafka";

    public final File dir;

    private final RollingStrategy rollingStategy;

    final int flushInterval;

    final boolean needRecovery;

    ///////////////////////////////////////////////////////////////////////
    private final Object lock = new Object();

    private final AtomicInteger unflushed = new AtomicInteger(0);

    private final AtomicLong lastflushedTime = new AtomicLong(System.currentTimeMillis());

    public final String name;

    private final LogStats logStats = new LogStats(this);

    private final SegmentList segments;
    //todo:alfred:add IndexSegmentList
    private  IndexSegmentList idxSegments;


    public final int partition;

    public Log(File dir, //
            int partition,//
            RollingStrategy rollingStategy,//
            int flushInterval, //
            boolean needRecovery) throws IOException {
        super();
        this.dir = dir;
        this.partition = partition;
        this.rollingStategy = rollingStategy;
        this.flushInterval = flushInterval;
        this.needRecovery = needRecovery;
        this.name = dir.getName();
        this.logStats.setMbeanName("jafka:type=jafka.logs." + name);
        Utils.registerMBean(logStats);
        segments = loadSegments();
        //init index segment list
        loadIdxSegments(segments);
    }



    private SegmentList loadSegments() throws IOException {
        List<LogSegment> accum = new ArrayList<LogSegment>();
        File[] ls = dir.listFiles(new FileFilter() {

            public boolean accept(File f) {
                return f.isFile() && f.getName().endsWith(FileSuffix);
            }
        });
        logger.info("loadSegments files from [" + dir.getAbsolutePath() + "]: " + ls.length);
        int n = 0;
        for (File f : ls) {
            n++;
            String filename = f.getName();
            long start = Long.parseLong(filename.substring(0, filename.length() - FileSuffix.length()));
            final String logFormat = "LOADING_LOG_FILE[%2d], start(offset)=%d, size=%d, path=%s";
            logger.info(String.format(logFormat, n, start, f.length(), f.getAbsolutePath()));
            FileMessageSet messageSet = new FileMessageSet(f, false);
            accum.add(new LogSegment(f, messageSet, start));
        }
        if (accum.size() == 0) {
            // no existing segments, create a new mutable segment
            File newFile = new File(dir, Log.nameFromOffset(0));
            FileMessageSet fileMessageSet = new FileMessageSet(newFile, true);
            accum.add(new LogSegment(newFile, fileMessageSet, 0));
        } else {
            // there is at least one existing segment, validate and recover them/it
            // sort segments into ascending order for fast searching
            Collections.sort(accum);
            validateSegments(accum);
        }
        //
        LogSegment last = accum.remove(accum.size() - 1);
        last.getMessageSet().close();
        logger.info("Loading the last segment " + last.getFile().getAbsolutePath() + " in mutable mode, recovery " + needRecovery);
        //todo:alfred: check whether index files are correct
        LogSegment mutable = new LogSegment(last.getFile(), new FileMessageSet(last.getFile(), true, new AtomicBoolean(
                needRecovery)), last.start());
        accum.add(mutable);
        return new SegmentList(name, accum);
    }


    //todo:alfred: realize this method，only load the existing index segments.If there is not a corresponding index file for a jafka file,skip.
    private void loadIdxSegments(SegmentList segments) throws IOException {
        List<LogSegment> segmentsList = segments.getView();
        List<LogIndexSegment> idxSegmentList = new ArrayList<LogIndexSegment>();

        boolean roll = false;
        for(LogSegment logSegment:segmentsList){
            String idxFilePath = logSegment.getFile().getAbsolutePath();
            idxFilePath += LogIndexSegment.FILE_SUFFIX;
            File file = new File(idxFilePath);
            if(!file.exists()){
                logger.warn("Loading index file ["+idxFilePath+"] failed => not exists!");
                //if this is the last logsegment
                if(logSegment.isMutable()){
                    //If the last log segment file still have no index file, create a new jafka file with index file
                    if(logSegment.size() != 0){
                        roll = true;
                    }else{
                        //create an index file for this segment file
                        file.createNewFile();
                        //FileChannel channel = new RandomAccessFile(file,"rw").getChannel();
                        FileChannel channel = Utils.openChannel(file,true);
                        LogIndexSegment idxSegment = new LogIndexSegment(file,channel,true,logSegment,false);
                        idxSegmentList.add(idxSegment);
                    }
                    break;
                }
            }else{
                logger.info("Loading index file [" + idxFilePath + "] succeed!");
                //FileChannel channel = logSegment.isMutable()?new RandomAccessFile(file,"rw").getChannel():new FileInputStream(file).getChannel();
                FileChannel channel = Utils.openChannel(file,logSegment.isMutable());
                LogIndexSegment idxSeg = new LogIndexSegment(file,channel,logSegment.isMutable(),logSegment,logSegment.isMutable()?needRecovery:false);
                idxSegmentList.add(idxSeg);
            }
        }

        idxSegments = new IndexSegmentList(name,idxSegmentList);
        //roll if the last segment have no index file to keep the message number are same in log segment and index segment.
        if(roll){
            roll();
        }
    }

    /**
     * Check that the ranges and sizes add up, otherwise we have lost some data somewhere
     */
    //todo:alfred: how to validate index segments
    //index segments中是针对每一条消息创建的索引，如果有压缩消息的话，如何获取所有的消息数目？
    //index size%64==0可以校验准确性，保证索引，但校验是否
    private void validateSegments(List<LogSegment> segments) {
        synchronized (lock) {
            for (int i = 0; i < segments.size() - 1; i++) {
                LogSegment curr = segments.get(i);
                LogSegment next = segments.get(i + 1);
                if (curr.start() + curr.size() != next.start()) {
                    throw new IllegalStateException("The following segments don't validate: " + curr.getFile()
                            .getAbsolutePath() + ", " + next.getFile().getAbsolutePath());
                }
            }
        }
    }

    public int getNumberOfSegments() {
        return segments.getView().size();
    }
    /**
     * delete all log segments in this topic-partition <br/>
     * The log directory will be removed also.
     * @return segment counts deleted
     */
    public int delete() {
        close();
       int count = segments.trunc(Integer.MAX_VALUE).size();
       Utils.deleteDirectory(dir);
       return count;
    }
    
    public void close() {
        synchronized (lock) {
            for (LogSegment seg : segments.getView()) {
                try {
                    seg.getMessageSet().close();
                } catch (IOException e) {
                    logger.error("close file message set failed", e);
                }
            }

            for(LogIndexSegment idxSeg : idxSegments.getView()){
                try {
                    idxSeg.close();
                } catch (IOException e) {
                    logger.error("close index file failed!",e);
                }
            }
        }
        //unregisterMBean
        Utils.unregisterMBean(this.logStats);
    }

    /**
     * read messages beginning from offset
     * 
     * @param offset next message offset
     * @param length the max package size
     * @return a MessageSet object with length data or empty
     * @see MessageSet#Empty
     * @throws IOException
     */
    public MessageSet read(long offset, int length) throws IOException {
        List<LogSegment> views = segments.getView();
        LogSegment found = findRange(views, offset, views.size());
        if (found == null) {
            if (logger.isTraceEnabled()) {
                logger.trace(format("NOT FOUND MessageSet from Log[%s], offset=%d, length=%d", name, offset, length));
            }
            return MessageSet.Empty;
        }
        return found.getMessageSet().read(offset - found.start(), length);
    }

    /**
     * read the messages after some time
     * @param time  milliseconds
     * @param length bytes
     * @return
     * @throws IOException
     */
    //todo:alfred:test this method
    public MessageSet readByTime(long time, int length) throws IOException {
        LogIndexSegment idxSegment = idxSegments.getLogIndexSegmentByTime(time);
        long offset = idxSegment.getOffsetByTime(time);
        return idxSegment.getLogSegment().getMessageSet().read(offset,length);
    }


    public List<Long> append(ByteBufferMessageSet messages) {
        //validate the messages
        int numberOfMessages = 0;
        for (MessageAndOffset messageAndOffset : messages) {
            if (!messageAndOffset.message.isValid()) {
                throw new InvalidMessageException();
            }
            numberOfMessages += 1;
        }
        //
        BrokerTopicStat.getBrokerTopicStat(getTopicName()).recordMessagesIn(numberOfMessages);
        BrokerTopicStat.getBrokerAllTopicStat().recordMessagesIn(numberOfMessages);
        logStats.recordAppendedMessages(numberOfMessages);

        // truncate the message set's buffer upto validbytes, before appending it to the on-disk log
        ByteBuffer validByteBuffer = messages.getBuffer().duplicate();
        long messageSetValidBytes = messages.getValidBytes();
        if (messageSetValidBytes > Integer.MAX_VALUE || messageSetValidBytes < 0) throw new InvalidMessageSizeException(
                "Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");

        validByteBuffer.limit((int) messageSetValidBytes);
        ByteBufferMessageSet validMessages = new ByteBufferMessageSet(validByteBuffer);

        // they are valid, insert them in the log
        synchronized (lock) {
            try {
                LogSegment lastSegment = segments.getLastView();
                //written bytes and the total file size before written
                long[] writtenAndOffset = lastSegment.getMessageSet().append(validMessages);
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("[%s,%s] save %d messages, bytes %d", name, lastSegment.getName(),
                            numberOfMessages, writtenAndOffset[0]));
                }

                Iterator<MessageAndOffset> iter = validMessages.internalIterator(true);
                LogIndexSegment idxSegment = idxSegments.getLastView();
                while(iter.hasNext()){
                    MessageAndOffset msgAndOffset = iter.next();
                    //the message set in this ByteBufferMessageSet
                    long msgOffsetInSet = msgAndOffset.offset - msgAndOffset.message.serializedSize();
                    long fileOffset = writtenAndOffset[1]+msgOffsetInSet;
                    logger.info("**********" + String.format("written[%d],msgOffset[%d],fileoffset[%d]---(%s)", writtenAndOffset[0], msgOffsetInSet, fileOffset, this.dir.getName()));
                    if(msgAndOffset.message.compressionCodec() != CompressionCodec.NoCompressionCodec){
                        //if this message is a compressed message,add all its messages to the index with this message file offset
                       ByteBufferMessageSet msgSets = CompressionUtils.decompress(msgAndOffset.message);
                       for(MessageAndOffset tmpMsgAndOffset:msgSets){
                           idxSegment.append(tmpMsgAndOffset.message.messageId(),fileOffset);
                       }
                    }else{
                        idxSegment.append(msgAndOffset.message.messageId(),fileOffset);
                    }

                }

                maybeFlush(numberOfMessages);
                maybeRoll(lastSegment);

            } catch (IOException e) {
                logger.fatal("Halting due to unrecoverable I/O error while handling producer request", e);
                Runtime.getRuntime().halt(1);
            } catch (RuntimeException re) {
                throw re;
            }
        }
        return (List<Long>) null;
    }

    /**
     * check the log whether needing rolling
     * 
     * @param lastSegment the last file segment
     * @throws IOException any file operation exception
     */
    private void maybeRoll(LogSegment lastSegment) throws IOException {
        if (rollingStategy.check(lastSegment)) {
            roll();
        }
    }

    private void roll() throws IOException {
        synchronized (lock) {
            long newOffset = nextAppendOffset();
            File newFile = new File(dir, nameFromOffset(newOffset));
            if (newFile.exists()) {
                logger.warn("newly rolled logsegment " + newFile.getName() + " already exists, deleting it first");
                if (!newFile.delete()) {
                    logger.error("delete exist file(who will be created for rolling over) failed: " + newFile);
                    throw new RuntimeException(
                            "delete exist file(who will be created for rolling over) failed: " + newFile);
                }
            }
            segments.append(new LogSegment(newFile, new FileMessageSet(newFile, true), newOffset));

            //create the index file
            File idxFile = new File(newFile.getAbsolutePath()+LogIndexSegment.FILE_SUFFIX);
            if(idxFile.exists()){
                logger.warn("newly rolled index segment file "+idxFile.getName()+" already exists, deleting it first");
                if(!idxFile.delete()){
                    logger.error("delete exist file failed:"+idxFile);
                    throw new RuntimeException("delete exist file failed:"+idxFile);
                }
            }
            idxFile.createNewFile();
            //FileChannel channel = new RandomAccessFile(idxFile,"rw").getChannel();
            FileChannel channel = Utils.openChannel(idxFile,true);
            LogIndexSegment idxSeg = new LogIndexSegment(idxFile,channel,true,segments.getLastView(),false);
            idxSegments.append(idxSeg);

            logger.info("Rolling log '" + name + "' to " + newFile.getName() + " and create its index file!");
        }
    }

    /**
     * @return
     * @throws IOException
     */
    private long nextAppendOffset() throws IOException {
        flush();
        LogSegment lastView = segments.getLastView();
        return lastView.start() + lastView.size();
    }

    /**
     * @param numberOfMessages
     * @throws IOException
     */
    private void maybeFlush(int numberOfMessages) throws IOException {
        if (unflushed.addAndGet(numberOfMessages) >= flushInterval) {
            flush();
        }
    }

    /**
     * Flush this log file to the physical disk
     * 
     * @throws IOException
     */
    public void flush() throws IOException {
        if (unflushed.get() == 0) return;

        synchronized (lock) {
            if (logger.isTraceEnabled()) {
                logger.debug("Flushing log '" + name + "' last flushed: " + getLastFlushedTime() + " current time: " + System
                        .currentTimeMillis());
            }
            segments.getLastView().getMessageSet().flush();
            //flush index too
            idxSegments.getLastView().flush();
            unflushed.set(0);
            lastflushedTime.set(System.currentTimeMillis());
        }
    }

    ///////////////////////////////////////////////////////////////////////
    /**
     * Find a given range object in a list of ranges by a value in that range. Does a binary
     * search over the ranges but instead of checking for equality looks within the range.
     * Takes the array size as an option in case the array grows while searching happens
     * 
     * TODO: This should move into SegmentList.scala
     */
    public static <T extends Range> T findRange(List<T> ranges, long value, int arraySize) {
        if (ranges.size() < 1) return null;
        T first = ranges.get(0);
        T last = ranges.get(arraySize - 1);
        // check out of bounds
        if (value < first.start() || value > last.start() + last.size()) {
            throw new OffsetOutOfRangeException("offset " + value + " is out of range");
        }

        // check at the end
        if (value == last.start() + last.size()) return null;

        int low = 0;
        int high = arraySize - 1;
        while (low <= high) {
            int mid = (high + low) / 2;
            T found = ranges.get(mid);
            if (found.contains(value)) {
                return found;
            } else if (value < found.start()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    public static <T extends Range> T findRange(List<T> ranges, long value) {
        return findRange(ranges, value, ranges.size());
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number
     * with zeros so that ls sorts the files numerically
     */
    public static String nameFromOffset(long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset) + Log.FileSuffix;
    }

    public String getTopicName() {
        return this.name.substring(0, name.lastIndexOf("-"));
    }

    public long getLastFlushedTime() {
        return lastflushedTime.get();
    }

    /**
     * all message size in the broker(some old messages has been deleted)
     * 
     * @return effected message size
     */
    public long size() {
        int size = 0;
        for (LogSegment seg : segments.getView()) {
            size += seg.size();
        }
        return size;
    }

    /**
     * get the current high watermark of the log
     */
    public long getHighwaterMark() {
        return segments.getLastView().size();
    }

    /**
     * Delete any log segments matching the given predicate function
     * 
     * @throws IOException
     */
    List<LogSegment> markDeletedWhile(LogSegmentFilter filter) throws IOException {
        synchronized (lock) {
            List<LogSegment> view = segments.getView();
            List<LogSegment> deletable = new ArrayList<LogSegment>();
            for (LogSegment seg : view) {
                if (filter.filter(seg)) {
                    deletable.add(seg);
                }
            }
            for (LogSegment seg : deletable) {
                seg.setDeleted(true);
            }
            int numToDelete = deletable.size();
            //
            // if we are deleting everything, create a new empty segment
            if (numToDelete == view.size()) {
                if (view.get(numToDelete - 1).size() > 0) {
                    roll();
                } else {
                    // If the last segment to be deleted is empty and we roll the log, the new segment will have the same
                    // file name. So simply reuse the last segment and reset the modified time.
                    view.get(numToDelete - 1).getFile().setLastModified(System.currentTimeMillis());
                    numToDelete -= 1;
                }
            }
            return segments.trunc(numToDelete);
        }
    }

    public List<Long> getOffsetsBefore(OffsetRequest offsetRequest) {
        List<LogSegment> logSegments = segments.getView();
        final LogSegment lastLogSegent = segments.getLastView();
        final boolean lastSegmentNotEmpty = lastLogSegent.size() > 0;
        List<KV<Long, Long>> offsetTimes = new ArrayList<KV<Long, Long>>();
        for (LogSegment ls : logSegments) {
            offsetTimes.add(new KV<Long, Long>(//
                    ls.start(), ls.getFile().lastModified()));
        }
        if (lastSegmentNotEmpty) {
            offsetTimes.add(new KV<Long, Long>(lastLogSegent.start() + lastLogSegent.getMessageSet().highWaterMark(),
                    System.currentTimeMillis()));
        }
        int startIndex = -1;
        final long requestTime = offsetRequest.time;
        if (requestTime == OffsetRequest.LATES_TTIME) {
            startIndex = offsetTimes.size() - 1;
        } else if (requestTime == OffsetRequest.EARLIES_TTIME) {
            startIndex = 0;
        } else {
            boolean isFound = false;
            startIndex = offsetTimes.size() - 1;
            for (; !isFound && startIndex >= 0; startIndex--) {
                if (offsetTimes.get(startIndex).v <= requestTime) {
                    isFound = true;
                }
            }
        }
        final int retSize = Math.min(offsetRequest.maxNumOffsets, startIndex + 1);
        final List<Long> ret = new ArrayList<Long>(retSize);
        for (int j = 0; j < retSize; j++) {
            ret.add(offsetTimes.get(startIndex).k);
            startIndex -= 1;
        }
        return ret;
    }

    @Override
    public long getOffsetUsingIndex(OffsetRequest offsetRequest) {
        LogIndexSegment idxSegment = idxSegments.getLogIndexSegmentByTime(offsetRequest.time);
        if(idxSegment == null)
            return -1;
        try {
            return idxSegment.getOffsetByTime(offsetRequest.time);
        } catch (IOException e) {
            logger.error("io error",e);
            return -1;
        }
    }

    @Override
    public String toString() {
        return "Log [dir=" + dir + ", lastflushedTime=" + //
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(lastflushedTime.get())) + "]";
    }

    public long getTotalOffset() {
        LogSegment lastView = segments.getLastView();
        return lastView.start() + lastView.size();
    }

    public long getTotalAddressingOffset() {
        LogSegment lastView = segments.getLastView();
        return lastView.start() + lastView.addressingSize();
    }

    public long getLastSegmentAddressingSize() {
        return segments.getLastView().addressingSize();
    }

    public IndexSegmentList getIdxSegments(){
        return idxSegments;
    }

    public boolean canIdxTruncDirectly(){
        return this.idxSegments.size() == this.segments.size();
    }

}
