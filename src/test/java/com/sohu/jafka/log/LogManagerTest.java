package com.sohu.jafka.log;

import com.sohu.jafka.BaseJafkaServer;
import com.sohu.jafka.server.ServerConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * @author rockybean(smilingrockybean@gmail.com)
 */
public class LogManagerTest extends BaseJafkaServer{


    private ServerConfig createServerConfig(){
        Properties props = new Properties();
        props.setProperty("log.dir","/Users/rockybean/Documents/workspace/java/jafka-data/data1");
        ServerConfig config = new ServerConfig(props);
        return config;
    }

    @Test
    public void testLogManagerStart(){
        LogManager logManager = new LogManager(createServerConfig(),null,1000,1000,true);
        try {
            logManager.load();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //logManager.startup();
        logManager.close();
    }


}
