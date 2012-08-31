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
        props.setProperty("log.dir","your log file");
        props.put("log.retention.size","10000");
        ServerConfig config = new ServerConfig(props);
        return config;
    }

    /*
    @Test
    public void testLogManagerLoadAndCleanup() throws IOException {
        LogManager logManager = new LogManager(createServerConfig(),null,1000000,1000000,false);
        try {
            logManager.load();
        } catch (IOException e) {
        logger.error(e);
        }
        //test clean up
        logManager.cleanupLogs();
        //logManager.startup();
        logManager.close();
    }
    */


}
