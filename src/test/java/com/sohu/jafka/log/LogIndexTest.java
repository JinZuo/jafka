package com.sohu.jafka.log;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.Jafka;
import com.sohu.jafka.api.FetchRequest;
import com.sohu.jafka.consumer.SimpleConsumer;
import com.sohu.jafka.message.ByteBufferMessageSet;
import com.sohu.jafka.message.Message;
import com.sohu.jafka.message.MessageAndOffset;
import com.sohu.jafka.producer.Producer;
import com.sohu.jafka.producer.ProducerConfig;
import com.sohu.jafka.producer.StringProducerData;
import com.sohu.jafka.producer.serializer.StringEncoder;

import java.io.IOException;
import java.util.Properties;

/**
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class LogIndexTest extends BaseJafkaServer{

    private Jafka jafka;

    private void startJafka(){
        jafka = createJafka();
    }


    private void closeJafka(){
        close(jafka);
    }


    private void produceMessages(String topic){
       Properties props = new Properties();
       props.setProperty("broker.list","0:localhost:9092");
       props.setProperty("serializer.class", StringEncoder.class.getName());

       Producer procuer = new Producer(new ProducerConfig(props));

        for(int i = 0;i < 100;i++){
            procuer.send(new StringProducerData(topic).add("i'm message -> "+i));
        }

        procuer.close();
    }

    private void consumerMessages(String topic) throws IOException {
        SimpleConsumer consumer = new SimpleConsumer("localhost",9092);
        FetchRequest req = new FetchRequest(topic,0,0,1000*1000);
        ByteBufferMessageSet messageSet = consumer.fetch(req);
        for(MessageAndOffset msgOff:messageSet){
            Message msg = msgOff.message;
            if(msg.getMessageId() == null){
                System.out.println("no id!!");
            }else{
                System.out.println("id is "+msg.getMessageId());
            }
            System.out.println(new String(msg.payload().array()));
        }
    }



}
