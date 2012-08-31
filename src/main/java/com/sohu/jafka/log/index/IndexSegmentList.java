package com.sohu.jafka.log.index;

import com.sohu.jafka.log.LogSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * all index segments including all readable files and last writable file.
 * @author: rockybean(smilingrockybean@gmail.com)
 * Date: 12-8-24
 */
public class IndexSegmentList {

    private AtomicReference<List<LogIndexSegment>> segmentsList;
    private String topicPartitionName;

    public IndexSegmentList(String name,List<LogIndexSegment> segments){
        this.topicPartitionName = name;
        this.segmentsList = new AtomicReference<List<LogIndexSegment>>(segments);
    }

    public IndexSegmentList(){}

    /**
     * find the index file containing time
     * @param time
     * @return
     */
    public LogIndexSegment getLogIndexSegmentByTime(long time){
        int low = 0;
        int high = segmentsList.get().size()-1;

        if(high < 0){
            return null;
        }

        if(time <= segmentsList.get().get(low).getStartTime()){
            return segmentsList.get().get(low);
        }

        int mid;
        while(low <= high){
            mid = (low + high)/2;
            LogIndexSegment tmp = segmentsList.get().get(mid);
            int containRes = tmp.contains(time);
            if(containRes == 0){
                return tmp;
            }else if(containRes < 0){
                high = mid - 1;
            }else{
                low = mid + 1;
            }
        }
        return null;
    }

    public List<LogIndexSegment> trunc(int count) {
        if(count < 0){
            throw new IllegalStateException("trunc count must be positive!");
        }

        while(true){
            List<LogIndexSegment> currLst = getView();
            int newLength = Math.max(currLst.size()-count,0);
            List<LogIndexSegment> updatedLst = new ArrayList<LogIndexSegment>(currLst.subList(Math.min(count,currLst.size()-1),currLst.size()));
            if(segmentsList.compareAndSet(currLst,updatedLst)){
                return currLst.subList(0,currLst.size()-newLength);
            }
        }
    }

    //
    public List<LogIndexSegment> trunc(List<LogSegment> segments) {
        if(segments.size() == 0){
            return null;
        }
        while(true){
            List<LogIndexSegment> currLst = getView();
            int startSeg = 0;
            for(LogSegment ls:segments){
                if(ls == currLst.get(0).getLogSegment()){
                    break;
                }
                startSeg++;
            }
            int truncCount = segments.size() - startSeg;
            int newLength = Math.max(currLst.size()-truncCount,0);
            List<LogIndexSegment> updatedLst = new ArrayList<LogIndexSegment>(currLst.subList(Math.max(truncCount,currLst.size()-1),currLst.size()));
            if(segmentsList.compareAndSet(currLst,updatedLst)){
                return currLst.subList(0,currLst.size()-newLength);
            }

        }
    }

    public int size(){
        return segmentsList.get().size();
    }

    public List<LogIndexSegment> getView(){
        return segmentsList.get();
    }

    public void append(LogIndexSegment idxSeg) {
        while(true){
            List<LogIndexSegment> idxSegLst = new ArrayList<LogIndexSegment>(getView());
            idxSegLst.add(idxSeg);

            if(segmentsList.compareAndSet(getView(),idxSegLst)){
                return;
           }
         }
    }

    public LogIndexSegment getLastView() {
        return segmentsList.get().get(size()-1);
    }
}
