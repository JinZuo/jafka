package com.sohu.jafka.message;

import org.junit.Test;

/**
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class MessageIdCenterTest {

    /**
         *compare the debug info and the output message
         */
    @Test
    public void testMessageIdGeneration(){
        long id = MessageIdCenter.generateId(1);
        MessageId msgId = new MessageId(id);
        System.out.println(msgId);
    }


}
