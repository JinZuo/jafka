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

package com.sohu.jafka.consumer;

import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.api.MultiFetchRequest;
import com.sohu.jafka.api.MultiFetchResponse;
import com.sohu.jafka.api.OffsetRequest;
import com.sohu.jafka.common.ErrorMapping;
import com.sohu.jafka.common.annotations.ClientSide;
import com.sohu.jafka.common.annotations.ThreadSafe;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.network.Receive;
import com.sohu.jafka.utils.KV;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple message consumer
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ThreadSafe
@ClientSide
public class SimpleConsumer extends SimpleOperation implements IConsumer {

    public SimpleConsumer(String host, int port) {
        super(host,port);
    }

    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        super(host, port, soTimeout, bufferSize);
    }


    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        KV<Receive, ErrorMapping> response = send(request);
        return new ByteBufferMessageSet(response.k.buffer(), request.offset, response.v);
    }

    /**
     * return the offset contained in the jafka file name before time
     * @param topic message topic
     * @param partition topic partition
     * @param time the log file created time {@link OffsetRequest#time}
     * @param maxNumOffsets the number of offsets
     * @return
     * @throws IOException
     */
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException {
        KV<Receive, ErrorMapping> response = send(new OffsetRequest(topic, partition, time, maxNumOffsets));
        return OffsetRequest.deserializeOffsetArray(response.k.buffer());
    }

    public long getOffset(String topic, int partition, long time) throws IOException {
        if(Message.CurrentMagicValue < Message.MAGIC_VERSION_WITH_ID)
            throw new IllegalStateException("Your client message version is too old! Please upgrade your files!");
       KV<Receive, ErrorMapping> response = send(new OffsetRequest(topic,partition,time,-1));
        return OffsetRequest.deserializeOffsetArray(response.k.buffer())[0];
    }

    public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        KV<Receive, ErrorMapping> response = send(new MultiFetchRequest(fetches));
        List<Long> offsets = new ArrayList<Long>();
        for (FetchRequest fetch : fetches) {
            offsets.add(fetch.offset);
        }
        return new MultiFetchResponse(response.k.buffer(), fetches.size(), offsets);
    }

    @Override
    public long getLatestOffset(String topic, int partition) throws IOException {
        long[] result = getOffsetsBefore(topic, partition, -1, 1);
        return result.length == 0 ? -1 : result[0];
    }
}
