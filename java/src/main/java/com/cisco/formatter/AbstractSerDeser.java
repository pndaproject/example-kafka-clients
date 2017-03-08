/*
 * Name       : AbstractSerDeser
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

package com.cisco.formatter;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.kafka.common.errors.SerializationException;
import com.cisco.shared.Constants;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;


/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */

public abstract class AbstractSerDeser {
  static final Logger logger = Logger.getLogger(AbstractSerDeser.class);
  protected String schemaFile;
  protected Integer schemaId;
  protected Boolean extraHeader;
  protected Schema schema;
  protected GenericRecord emptyRefRecord = null;

  protected byte[] removeSchemaIdHeader(byte[] value) {
        logger.debug("removeSchemaIdHeader start");
        if (value == null) {
            throw new SerializationException("Null value - nothing to do");
        }
        // Check first byte is the MAGIC BYTE
        ByteBuffer buffer = ByteBuffer.allocate(0);
        try {
            buffer = ByteBuffer.wrap(value);
        } catch (Exception e) {
            logger.error("ByteBuffer.wrap with payload ["+value+"] failed: "+e);
        }
        if (buffer.get() != Constants.TEST_MAGIC_BYTE) {
            logger.error("buffer.get() ["+value+"] is not ["+Constants.TEST_MAGIC_BYTE+"]");
            throw new SerializationException("Unknown magic byte!");
        }
        // Then, check the schema ID (shift of 4)
        int id = buffer.getInt();
        // Shall we put a constraint on the scheme or retrieve it based on this id
        // Improvement for future platform objectives with Schema Registry
        logger.debug("Schema Id read from message: ["+id+"]");
        logger.debug("Schema Id parameter: ["+schemaId+"]");
        byte[] result = new byte[buffer.remaining()];
        buffer.duplicate().get(result);
        return result;
  }



  protected byte[] getSchemaIdHeader() {
        logger.debug("getSchemaIdHeader() start");
        int id = -1;
        ByteArrayOutputStream outHeader = null;
        try {
            outHeader = new ByteArrayOutputStream();
            outHeader.write(Constants.TEST_MAGIC_BYTE);
            outHeader.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            outHeader.close();
        } catch(Exception e){
            logger.error("error:: EXCEPTION: "+e);
        }

        try {
            if (outHeader != null) {
                outHeader.close();
            }
        } catch(Exception e){
            logger.error(e);
        }
        if (outHeader != null){
            return outHeader.toByteArray();
        }
        else{
            return null;
        }
  }

  protected byte[] serializeImpl(GenericRecord record) {
        logger.debug("serializeImpl start");
        ByteArrayOutputStream stream=null;
        byte[] ob=null;
        ByteArrayOutputStream result = new ByteArrayOutputStream( );
        try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            stream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null); 
            writer.write(record, encoder);
            encoder.flush();
            ob = stream.toByteArray();
            if (this.extraHeader == true) {
              logger.info("serializeImpl: extraHeader TRUE");
              byte[] extraHeaderBytes=getSchemaIdHeader();
              result.write(extraHeaderBytes);
            }
            else {
              logger.debug("serializeImpl: extraHeader FALSE");
            }
            result.write(ob);
        }catch (Exception ex) {
            logger.error("serializeImpl failed: " + ex);
            throw new SerializationException("serializeImpl failed");
        }finally{
            try {
                if (stream != null) {
                    stream.close();
                }
            }catch(Exception e) {
                    logger.error("failed to close ByteArrayOutputStream stream: " +e);
            }
        }
        
        return result.toByteArray();
    } // end toBytes()

  public Object deserializeBin(byte[] value) {
        logger.debug("deserializeBin start");
        GenericRecord brec=null;
        byte[] output="".getBytes();
        String outputStr="";
        byte[] message = null;
        Object decoded = null;

        if (this.extraHeader) {
          logger.debug("deserializeBin: remove extra header first");
          try {
               // Remove extra header bytes
                message = removeSchemaIdHeader(value);
                logger.debug("debug: rcv: cleanup ["+Arrays.toString(message)+"]");
                value=message;

          } catch(Exception e) {
                logger.error("message ignored [" +value+"] - failed decoding extra header");
                logger.error(e);
                throw new SerializationException("failed decoding extra header");
          }
        }

        try { 
            ByteArrayInputStream bais=new ByteArrayInputStream(value);
            DatumReader<Object> reader=new GenericDatumReader<Object>(schema);
            BinaryDecoder dec=DecoderFactory.get().binaryDecoder(bais,null);
            decoded=reader.read(null,dec);
            outputStr=decoded.toString();
        }
        catch (Exception ex) {
            logger.error("deserializeBin failed: " + ex);
            throw new SerializationException("deserializeBin failed");
        }
        logger.debug("Output ["+outputStr+"]");
        return decoded;
  } // End deserializeBin



 // Raised exception when binary decoding results in an emptyRecord
 public  Object deserialize(byte[] value, GenericRecord emptyRecord) {
        String outputStr="";
        String emptyRecStr="";
        Object decoded=null;

        try { 
            decoded=deserializeBin(value);
            outputStr=decoded.toString();
            emptyRecStr=emptyRecord.toString();
        }

        catch (Exception ex) {
            logger.error("deserializeBin failed: " + ex);
            throw new SerializationException("deserializeBin failed");
        }
        logger.debug("EmptyRec ["+emptyRecStr+"]");
        logger.info("Output ["+outputStr+"]");

        // Sometimes binary decoding may succeed but its does not match json schema
        // #output-check
        if (emptyRecStr.equalsIgnoreCase(outputStr)) {
            throw new SerializationException("empty Record");
        }
        return decoded;
  } // End deserialize


  protected Schema createSchema() {
    try {
        return new Schema.Parser().parse(new File(schemaFile));
    } catch (Exception ex) {
        logger.error(ex);
        throw new SerializationException("schema initialization failed");
    }
  }



protected Schema getSchema(final String schemaPath, final Integer schemaIdentiter, final Boolean extraHeaderFlag) {
        this.schemaFile = schemaPath;
        this.schemaId = schemaIdentiter;
        this.extraHeader = extraHeaderFlag;
        if (this.extraHeader) {
            AbstractSerDeser.logger.debug((Object)"schemaGet [header is true]");
        }
        else {
            AbstractSerDeser.logger.debug((Object)"schemaGet [header is false]");
        }
        return this.createSchema();
  }
}