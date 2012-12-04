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

package com.sohu.jafka.network.handlers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sohu.jafka.api.ProducerRequest;
import com.sohu.jafka.api.RequestKeys;
import com.sohu.jafka.log.ILog;
import com.sohu.jafka.log.LogManager;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.CompressionCodec;
import com.sohu.jafka.message.CompressionUtils;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.message.MessageIdCenter;
import com.sohu.jafka.mx.BrokerTopicStat;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.network.Send;
import com.sohu.jafka.server.Server;

import static java.lang.String.format;


/**
 * handler for producer request
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ProducerHandler extends AbstractHandler {

    final String errorFormat = "Error processing %s on %s:%d";

    public ProducerHandler(LogManager logManager) {
        super(logManager);
    }

    public Send handler(RequestKeys requestType, Receive receive) {
        final long st = System.currentTimeMillis();
        ProducerRequest request = ProducerRequest.readFrom(receive.buffer());
        if (logger.isDebugEnabled()) {
            logger.debug("Producer request " + request.toString());
        }
        handleProducerRequest(request);
        long et = System.currentTimeMillis();
        if (logger.isDebugEnabled()) {
            logger.debug("produce a message(set) cost " + (et - st) + " ms");
        }
        return null;
    }

    protected void handleProducerRequest(ProducerRequest request) {
        int partition = request.getTranslatedPartition(logManager);
        preProcessRequestByMessageMagicValue(request,partition);
        try {
            final ILog log = logManager.getOrCreateLog(request.topic, partition);
            log.append(request.messages);
            long messageSize = request.messages.getSizeInBytes();
            if (logger.isDebugEnabled()) {
                logger.debug(messageSize + " bytes written to logs " + log);
                for (MessageAndOffset m : request.messages) {
                    logger.trace("wrote message " + m.offset + " to disk");
                }
            }
            BrokerTopicStat.getInstance(request.topic).recordBytesIn(messageSize);
            BrokerTopicStat.getBrokerAllTopicStat().recordBytesIn(messageSize);
        } catch (RuntimeException e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw e;
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.error(format(errorFormat, request.getRequestKey(), request.topic, request.partition), e);
            } else {
                logger.error("Producer failed. " + e.getMessage());
            }
            BrokerTopicStat.getInstance(request.topic).recordFailedProduceRequest();
            BrokerTopicStat.getBrokerAllTopicStat().recordFailedProduceRequest();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * process the messages in request before they are written to disk.
     * @param request
     * @author rockybean
     */
    private void preProcessRequestByMessageMagicValue(ProducerRequest request,int partition) {
        //get id of this broker
        int brokerId = Server.brokerId;
        ByteBufferMessageSet messageSet = request.messages;
        //recreate a new ByteBufferMessageSet
        List<Message> msgLst = new ArrayList<Message>();
        Iterator<MessageAndOffset> outerItr = messageSet.internalIterator(true);
        while(outerItr.hasNext()){
            MessageAndOffset messageAndOffset = outerItr.next();
            Message msg = processMessage(messageAndOffset.message,brokerId,partition);
            msgLst.add(msg);
        }
        request.messages = new ByteBufferMessageSet(toArray(msgLst));
    }

    private Message processMessage(Message msg,int brokerId,int partition) {
        CompressionCodec codec = msg.compressionCodec();
        if(codec == CompressionCodec.NoCompressionCodec){
            byte magic = msg.magic();
            //change message by message version if it is necessary
            switch(magic){
                case Message.MAGIC_VERSION2:
                case Message.MAGIC_VERSION_WITH_ID:
                //case Message.NEW_VERSION...
                    //generate a messageId for the message
                    long msgId = MessageIdCenter.generateId(partition);
                    //get the data bytes in the message
                    ByteBuffer buffer = msg.payload();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    return new Message(brokerId,msgId,bytes);
            }
        }else{
            logger.info("use compression codec!");
            //compress message
            List<Message> msgLst = new ArrayList<Message>();
            Iterator<MessageAndOffset> itr = CompressionUtils.decompress(msg).internalIterator(true);
            while(itr.hasNext()){
                Message tmpMsg = processMessage(itr.next().message,brokerId,partition);
                msgLst.add(tmpMsg);
            }
            long msgId = MessageIdCenter.generateId(partition);
            return CompressionUtils.compress(toArray(msgLst), codec, brokerId, msgId);
        }
        return null;
    }

    private Message[] toArray(List<Message> msgLst) {
        Message[] arr = new Message[msgLst.size()];
        for(int i = 0;i < msgLst.size();i++){
            arr[i] = msgLst.get(i);
        }
        return arr;
    }
}
