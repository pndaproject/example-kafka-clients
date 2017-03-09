/*
 * Name       : AProducer
 * Created on : 14-12-2015
 * Author     : PNDA Team
 *
*/
/*
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.dataplatform;

import java.io.File;
import java.util.Properties;
import java.nio.charset.*;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
import com.cisco.formatter.IpUtil;
import com.cisco.shared.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.util.concurrent.ExecutorService;
import com.cisco.dataplatform.ConfigParser;
import java.io.IOException;

public class AProducer {
    
    static final Logger logger = Logger.getLogger(AProducer.class);
    private final Producer<String, Object> producer; 
    static final Properties mainConfig = new Properties();
    private  ExecutorService executor;

    public AProducer() {
        this.producer = new Producer<>(createProducerConfig());
    }

    private static ProducerConfig createProducerConfig() {
        Properties props = new Properties();
        // Mandatory ones
        try {
          props.put(""+Constants.PROP_MD_BROKER_LIST,                 mainConfig.getProperty(Constants.PROP_MD_BROKER_LIST)); 
          props.put(""+Constants.PROP_PRODUCER_TYPE,                  mainConfig.getProperty(Constants.PROP_PRODUCER_TYPE));
          props.put(""+Constants.PROP_PRODUCER_COMPRESSION_CODEC,     mainConfig.getProperty(Constants.PROP_PRODUCER_COMPRESSION_CODEC));
          String  msgSerialization=mainConfig.getProperty(Constants.PROP_SERIALIZATION);
          if (msgSerialization.equalsIgnoreCase(Constants.STR_SERIALIZATION_RAW)) {
            logger.debug("producer setting: no serialization required");
            props.put(""+Constants.PROP_PRODUCER_MSG_ENCODER,Constants.KAFKA_DEFAULT_MSG_ENCODER);
                        //Use the default binary encoder
          } else {
            logger.debug("producer setting: serialization required");
            //what Serializer to use when preparing the message for transmission to the Broker
            String configExtra=mainConfig.getProperty(Constants.PROP_AVRO_EXTRA_HEADER);
            props.put(""+Constants.PROP_PRODUCER_MSG_ENCODER,Constants.KAFKA_DATAPLATFORM_MSG_ENCODER);
            props.put(""+Constants.PROP_PRI_ENC_SCHEMA,mainConfig.getProperty(Constants.PROP_AVRO_SCHEMA));
            props.put(""+Constants.PROP_PRI_ENC_SCHEMAID,mainConfig.getProperty(Constants.PROP_AVRO_SCHEMAID));
            if ( configExtra.equalsIgnoreCase(Constants.MSG_HEADER_CONFLUENT_IO )) {
                props.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,(String) "true");
            }
            else {
                props.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,(String) "false");
            }   
          }

          props.put(""+Constants.PROP_KAFKA_GROUPID,                  mainConfig.getProperty(Constants.PROP_KAFKA_GROUPID ));

          // Serializer class for the key partition
          props.put(""+Constants.PROP_KEY_PARTITIONER_CLASS,      "kafka.serializer.StringEncoder");
          if (mainConfig.getProperty(Constants.PROP_PARTITIONER_CLASS) != null) {
            logger.debug("producer setting: partitioning required");
            // What class to use to determine which partition in the topic the msg is to be sent to
            props.put(""+Constants.PROP_PARTITIONER_CLASS,          mainConfig.getProperty(Constants.PROP_PARTITIONER_CLASS));
          }
          
          if (mainConfig.getProperty(Constants.PROP_PRODUCER_COMPRESSED_TOPIC) != null) {
            props.put(""+Constants.PROP_PRODUCER_COMPRESSED_TOPIC,  mainConfig.getProperty(Constants.PROP_PRODUCER_COMPRESSED_TOPIC));
          }

          if (mainConfig.getProperty(Constants.PROP_PRODUCER_MD_REFRESH) != null) {
            props.put(""+Constants.PROP_PRODUCER_MD_REFRESH,          mainConfig.getProperty(Constants.PROP_PRODUCER_MD_REFRESH));
          }
          // Aynsc producer only
          String asyncValue=Constants.STR_PRODUCER_TYPE_ASYNC;
          String producerType=mainConfig.getProperty(Constants.PROP_PRODUCER_TYPE);
          if (asyncValue.equalsIgnoreCase(producerType)) {
            props.put(""+Constants.PROP_PRODUCER_AS_Q_MAX_MS,       mainConfig.getProperty(Constants.PROP_PRODUCER_AS_Q_MAX_MS));
            props.put(""+Constants.PROP_PRODUCER_AS_Q_MAX_MSG,      mainConfig.getProperty(Constants.PROP_PRODUCER_AS_Q_MAX_MSG));
            props.put(""+Constants.PROP_PRODUCER_AS_Q_TIMEOUT,      mainConfig.getProperty(Constants.PROP_PRODUCER_AS_Q_TIMEOUT));
            props.put(""+Constants.PROP_PRODUCER_AS_Q_BATCH_MSG,    mainConfig.getProperty(Constants.PROP_PRODUCER_AS_Q_BATCH_MSG));
          }
        } 
          catch (Exception ex) {
            logger.error("unexpected error when preparing producer configuration: "+ex.getMessage());
        }

        return new ProducerConfig(props);
    }


    //-----------------------------------------
    public void run() {
        logger.info("run - Topic used ["+mainConfig.getProperty(Constants.PROP_KAFKA_TOPIC)+"]");

        String  topic=mainConfig.getProperty(Constants.PROP_KAFKA_TOPIC);
        String  msgSerialization=mainConfig.getProperty(Constants.PROP_SERIALIZATION);
        String   msgHostIp = null;
        String   producerSource=mainConfig.getProperty(Constants.PROP_PRODUCER_MSG_SRC);
        String   nicName=mainConfig.getProperty(Constants.PROP_PRODUCER_NIC_4_HOST_IP);
        String   srcHostIpFormat=mainConfig.getProperty(Constants.PROP_PRODUCER_FORMAT_IP);
        String   srcHostIpDefault=mainConfig.getProperty(Constants.PROP_PRODUCER_IP_DEFAULT);
        Schema  schema = null;

        logger.info("Message Host IP shall be resolved with NIC ["+nicName+"]");
        logger.info("Message Host IP shall resolved IP address with format ["+srcHostIpFormat+"]");
        logger.info("Message Host IP failure may result in using default IP ["+srcHostIpDefault+"]");

        try {
            // WARNING: for long live process, the value shall have a TTL so that it is renewed on a 
            // regular basis (detect network config change)
            IpUtil hostIpUtility = new IpUtil();
            msgHostIp  = hostIpUtility.getIpPerNic(nicName, srcHostIpFormat);
            if (msgHostIp == null) {
                msgHostIp=srcHostIpDefault;
            }
            if (msgHostIp == null || msgHostIp.length() == 0) {
                logger.error("Could not find an IP address for setting IP src into kafka messages");
            }
            logger.info("Message will use host_ip set to ["+msgHostIp+"]");
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        try {
            // Create a schema
            schema = new Schema.Parser().parse(new File(mainConfig.getProperty(Constants.PROP_AVRO_SCHEMA)));
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        try {
            while(true) {
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                String msgStr;
                KeyedMessage<String, Object> keyedMessage;
                do {
                    GenericRecord srec = new GenericData.Record(schema);
                    logger.info("Raw data (required) >> ");
                    msgStr = in.readLine();
                    if (msgStr.length() > 0) {
                        logger.info("Partition Key (optional) >> ");
                        String mykey = in.readLine();
                        if (msgSerialization.equalsIgnoreCase(Constants.STR_SERIALIZATION_RAW)) {
                            keyedMessage = new KeyedMessage<String, Object>(topic, mykey, msgStr.getBytes());
                        }
                        else {
                            // AVRO encoding
                            srec.put("src",       (String) producerSource);
                            srec.put("timestamp", System.currentTimeMillis());
                            srec.put("host_ip",   (String) msgHostIp);
                            srec.put("rawdata",   (ByteBuffer) ByteBuffer.wrap(msgStr.getBytes()));
                            keyedMessage = new KeyedMessage<String, Object>(topic, mykey, srec);
                        }
                        producer.send(keyedMessage);
                    }
                } while ( true ); // end do
            }
        }
        catch (Exception ex) {
            logger.error("unexpected error: ", ex);
        } 
    }

    public void shutdown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    public static void main(String[] args) {
        String configFile = args[0];

        logger.info("Properties file to be used: ["+configFile+"]");
        // Load the configuration for this program
        try {

            ConfigParser.parseConfig(args[0], mainConfig, logger, Constants.PROP_MD_BROKER_LIST);

        } catch (IOException e) {
            logger.error(e);
            System.exit(-1);
        } 

        AProducer aproducer = new AProducer();
        try {
            aproducer.run();
            while (true) {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_INT);
            }
        } catch (InterruptedException ie) {
            logger.error(ie);
            System.exit(-1);
        } catch (Exception ex) {
            logger.error(ex);
            System.exit(-1);
        }

        System.exit(0);
    }

}