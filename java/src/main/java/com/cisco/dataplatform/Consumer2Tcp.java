/*
 * Name       : Consumer2Tcp
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

import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import com.cisco.dataplatform.ConsumerProcess;
import com.cisco.formatter.DataplatformDecoder;
import com.cisco.shared.Constants;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.Whitelist;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.VerifiableProperties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.cisco.dataplatform.ConfigParser;

public class Consumer2Tcp {
    
    private static final Logger logger = Logger.getLogger(Consumer2Tcp.class);
    private final ConsumerConnector consumerConnector;
    private static final Properties mainConfig = new Properties();
    private static final Properties decoderProps = new Properties();
    private  ExecutorService executor;

    public Consumer2Tcp() {
        this.consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put(""+Constants.PROP_ZK_HOST,                 mainConfig.getProperty(Constants.PROP_ZK_HOST));
        props.put(""+Constants.PROP_KAFKA_GROUPID,           mainConfig.getProperty(Constants.PROP_KAFKA_GROUPID));
        props.put(""+Constants.PROP_ZK_SESSION_TIMEOUT,      mainConfig.getProperty(Constants.PROP_ZK_SESSION_TIMEOUT));
        props.put(""+Constants.PROP_ZK_SYNC_TIME,            mainConfig.getProperty(Constants.PROP_ZK_SYNC_TIME));
        props.put(""+Constants.PROP_ZK_AUTO_COMMIT_INTERVAL, mainConfig.getProperty(Constants.PROP_ZK_AUTO_COMMIT_INTERVAL));

        String  msgSerialization=mainConfig.getProperty(Constants.PROP_SERIALIZATION);

        // Raw is configured. We shall use the default kafka binary encoder
        if (msgSerialization.equalsIgnoreCase(Constants.STR_SERIALIZATION_RAW)) {
            logger.debug("producer setting: no serialization required");
            props.put(""+Constants.PROP_PRODUCER_MSG_ENCODER,Constants.KAFKA_DEFAULT_MSG_ENCODER);
                        //Use the default binary encoder
        } else {
            // Or Avro is required. We shall use our own class for serialization
            logger.debug("producer setting: serialization required");
            String configExtra=mainConfig.getProperty(Constants.PROP_AVRO_EXTRA_HEADER);
            // This is where we define our custom encoder
            props.put(""+Constants.PROP_PRODUCER_MSG_ENCODER,Constants.KAFKA_DEFAULT_MSG_ENCODER);
            // We put extra fields in the props, not used by kafka by our custom encoder
            decoderProps.put(""+Constants.PROP_PRI_ENC_SCHEMA,mainConfig.getProperty(Constants.PROP_AVRO_SCHEMA));
            decoderProps.put(""+Constants.PROP_PRI_ENC_SCHEMAID,mainConfig.getProperty(Constants.PROP_AVRO_SCHEMAID));
            if ( configExtra.equalsIgnoreCase(Constants.MSG_HEADER_CONFLUENT_IO )) {
                decoderProps.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,(String) "true");
            }
            else {
                decoderProps.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,(String) "false");
            }
            
        }

        return new ConsumerConfig(props);
    }

    public void run(int numThreads) {
        String topic = mainConfig.getProperty(Constants.PROP_KAFKA_TOPIC);
        logger.info("run - Topic used ["+topic+"]");
        
        // now launch all the threads
        executor = Executors.newFixedThreadPool(numThreads);
        int threadNumber = 0;

        String  msgSerialization=mainConfig.getProperty(Constants.PROP_SERIALIZATION);
        
        DataplatformDecoder msgDecoder = null;

        // Create a decoder when configured for working with Avro Serialization
        if (msgSerialization.equalsIgnoreCase(Constants.STR_SERIALIZATION_AVRO)) {
            VerifiableProperties vDecoderProps = new VerifiableProperties(decoderProps);
            msgDecoder = new DataplatformDecoder(vDecoderProps);
        }

        // Start init the consumer with a filter to specify a pattern for subscribing to 1 or more topics
        List<KafkaStream<byte[], byte[]>> streams =
                consumerConnector.createMessageStreamsByFilter(new Whitelist(topic),
                                                               new Integer(numThreads));
        for (final KafkaStream stream : streams) {
                executor.submit(new ConsumerProcess(stream,
                                                    threadNumber,
                                                    mainConfig,
                                                    msgDecoder));
                threadNumber++;
        }
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public static void main(String[] args) {
        String configFile = args[0];

        logger.info("Properties file to be used: ["+configFile+"]");
        // Load the configuration for this program
        try{ 
            
            ConfigParser.parseConfig(args[0], mainConfig, logger,Constants.PROP_ZK_HOST);

        } catch (Exception e) {
            logger.error(e);
            System.exit(-1);
        }

        Consumer2Tcp consumer = new Consumer2Tcp();     
        consumer.run(Integer.parseInt(mainConfig.getProperty(Constants.PROP_CONSUMER_THREADS)));

        try {
            while (true) {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_INT);
            }
        } catch (InterruptedException ie) {
            logger.error(ie);
        }
        consumer.shutdown();
        System.exit(0);
    }

}