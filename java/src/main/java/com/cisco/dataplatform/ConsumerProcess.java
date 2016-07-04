/*
 * Name       : ConsumerProcess
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

import org.apache.log4j.*;
import java.util.*;
import com.cisco.formatter.*;
import org.apache.avro.*;
import kafka.consumer.*;
import org.apache.avro.generic.*;
import kafka.message.*;
import java.io.*;
import com.cisco.shared.*;
import java.net.*;
import java.nio.ByteBuffer;
import org.codehaus.jackson.io.JsonStringEncoder;

public class ConsumerProcessException extends Exception {
    public ConsumerProcessException(String message) {
        super(message);
    }
}

public class ConsumerProcess implements Runnable
{
    static final Logger logger = Logger.getLogger(ConsumerProcess.class);
    private KafkaStream mStream;
    private int mThreadNumber;
    private Properties globalConfig;
    private Socket externalTcpConn;
    private OutputStream externalTcpOs;
    private DataplatformDecoder messageDecoder;
    
    public ConsumerProcess(final KafkaStream aStream, final int aThreadNumber, final Properties config, final DataplatformDecoder customDecoder) {
        this.globalConfig = new Properties();
        this.externalTcpConn = null;
        this.externalTcpOs = null;
        this.mThreadNumber = aThreadNumber;
        this.mStream = aStream;
        this.globalConfig = config;
        this.messageDecoder = customDecoder;
    }
    
    @Override
    public void run() {
        logger.info((Object)(this.mThreadNumber + ": start run()"));
        final ConsumerIterator<byte[], byte[]> it = (ConsumerIterator<byte[], byte[]>)this.mStream.iterator();
        GenericRecord emptyRefRecord = null;
        String msgRawType=Constants.STR_SERIALIZATION_RAW;
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File(this.globalConfig.getProperty(Constants.PROP_AVRO_SCHEMA)));
            emptyRefRecord = (GenericRecord)new GenericData.Record(schema);
            emptyRefRecord.put("src", (Object)"");
            emptyRefRecord.put("timestamp", (Object)"");
            emptyRefRecord.put("host_ip", (Object)"");
            emptyRefRecord.put("rawdata", (Object)"");
        }
        catch (Exception ex) {
            logger.error((Object)"The default init method for an empty record may not be valid with the schema: ", (Throwable)ex);
            throw new ConsumerProcessException(ex);
        }

        // Connect to the external TCP target
        if (this.globalConfig.getProperty(Constants.PROP_OUTPUT_HOST).length() > 0) {
          this.externalTcpConnect();
        }
        final String dropAVroEmptyMessage = this.globalConfig.getProperty(Constants.PROP_AVRO_DROP_MSG_EMPTY);
        final String msgSerialization = this.globalConfig.getProperty(Constants.PROP_SERIALIZATION);

        // Loop over Kafka received messages
        while (it.hasNext()) {
            final   MessageAndMetadata messageAndMetadata = it.next();
            final   byte[] message = (byte[])messageAndMetadata.message();
            byte[]  tcpbytes = null;
            String  deserDataStr = null;
            boolean errorBreak = false;

            // If not avro required, it means raw, so copy bytes to be sent
            if (msgSerialization.equalsIgnoreCase(Constants.STR_SERIALIZATION_RAW)) {
                logger.debug((Object)"No decoding required");
                tcpbytes = message;
                }else{
            
                // Avro required, call deserialize
                logger.debug((Object)"decoding required");
                String       tcpFieldSrc="";
                long         tcpFieldTimestamp;
                String       tcpFieldHostip="";
                String       tcpFieldRawdata="";
                Object       decoded = null;
                deserDataStr = null;
                try {
                    // Option to remove empty result record
                    if (msgRawType.equalsIgnoreCase(dropAVroEmptyMessage)) {
                        logger.debug((Object)"drop empty message option");
                        decoded = this.messageDecoder.deserialize(message, emptyRefRecord);
                    }
                    else { // no drop of empty result record
                        logger.debug((Object)"NO drop empty message option");
                        decoded = this.messageDecoder.deserializeBin(message);
                    }


                    // Sample Code for parsing the decoded Avro record fields
                    ByteBuffer rd = (ByteBuffer) ((GenericRecord)decoded).get("rawdata");
                    tcpFieldRawdata=new String(rd.array());
                    tcpFieldSrc=(String) ((GenericRecord)decoded).get("src").toString();
                    tcpFieldTimestamp=(long) ((GenericRecord)decoded).get("timestamp");
                    tcpFieldHostip=(String) ((GenericRecord)decoded).get("host_ip").toString();


                    // Sample Code for building the message we forward to the TCP target
                    deserDataStr =   "{\"timestamp\": "+tcpFieldTimestamp+", ";
                    deserDataStr +=  "\"src\": \""+tcpFieldSrc+"\", ";
                    deserDataStr +=  "\"host_ip\": \""+tcpFieldHostip+"\", ";

                    // Take care: could require special character encoding in the resulting field
                    JsonStringEncoder exampleEncoder = new JsonStringEncoder();
                    String exampleResult = new String(exampleEncoder.quoteAsString(tcpFieldRawdata));
                    deserDataStr +=  "\"rawdata\": \""+exampleResult+"\"}";


                    /* === #CustomImplementation#     ==================================
                       ================================================================

                       Do whatever you want with bytes before forward to the TCP target
                       ================================================================
                       === end #CustomImplementation# ================================== */

                    // Now we build the bytes we send to the TCP target
                    tcpbytes = deserDataStr.getBytes();   
                }
                catch (Exception e) {
                    logger.error((Object)(this.mThreadNumber + ": avroDecoder.fromBytes() failed: " + e));
                    errorBreak = true;
                }
            }
            if (!errorBreak) {
                try {
                    if (this.globalConfig.getProperty(Constants.PROP_OUTPUT_HOST).length() > 0) {
                        this.sendToExternalTcp(tcpbytes);
                    }
                    else {
                        logger.info((Object)("DumpIfNoTarget: " + deserDataStr));
                    }
                }
                catch (Exception e2) {
                    logger.error((Object)(this.mThreadNumber + ": sendToExternalTcp failed: " + e2));
                }
            }
            else {
                logger.error((Object)(this.mThreadNumber + ": message ignored for being sent to output"));
            }
        }
        logger.debug((Object)("Shutting down Thread: " + this.mThreadNumber));
    }
    

    // Function used to send bytes to an already connected target server
    private void sendToExternalTcp(final byte[] message) {
        logger.debug((Object)(this.mThreadNumber + ": " + new String(message)));
        while (true) {
            try {
                this.externalTcpOs.write(message);
            }
            catch (IOException e) {
                logger.error((Object)(this.mThreadNumber + ": sendToExternalTcp cnx error: " + e));
                logger.warn((Object)(this.mThreadNumber + ": will try to reconnect"));
                this.externalTcpConnect();
                continue;
            }
            break;
        }
    }
 
    // Function used to connect to a target server   
    private void externalTcpConnect() {
        logger.debug((Object)(this.mThreadNumber + ": start externalTcpConnect()"));
        while (true) {
            try {
                while (true) {
                    try {
                        this.externalTcpConn = new Socket(this.globalConfig.getProperty(Constants.PROP_OUTPUT_HOST), Integer.parseInt(this.globalConfig.getProperty("output.port")));
                        this.externalTcpOs = this.externalTcpConn.getOutputStream();
                    }catch (UnknownHostException e) {
                        logger.error((Object)(this.mThreadNumber + ": externalTcp cnx error host unknown: " + e));
                        Thread.sleep(Constants.TCP_THREAD_SLEEP_INT);
                    }catch (IOException e2) {
                        logger.error((Object)(this.mThreadNumber + ": externalTcp cnx error: " + e2));
                        Thread.sleep(Constants.TCP_THREAD_SLEEP_INT);
                    }finally{
                        this.externalTcpConn.close();
                    }
                }
            }
            catch (InterruptedException ie) {
                logger.error(ie);
                throw ie;
            }
        }
    }
}    
