/*
 * Name       : EncDecTest
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

import java.io.*;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import kafka.utils.VerifiableProperties;
import com.cisco.shared.Constants;
import com.cisco.formatter.DataplatformDecoder;
import com.cisco.formatter.DataplatformEncoder;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncDecTest {

    // For tests: need to get full path for loading scheme
    String curDir = Paths.get("").toAbsolutePath().toString();
    String        dpfSchemaPath= curDir + "/schema/dataplatform-raw.avsc";
    Integer       dpfSchemaId=23;
    GenericRecord emptyRefDpfRecord = null;
    Schema        schemaDpf=null;

    private static DataplatformDecoder generateProps(String scheme, Integer schemeId, String extraHeader) {
        class Local {};
        Properties props = new Properties();
        props.put(""+Constants.PROP_PRI_ENC_SCHEMA,scheme);
        props.put(""+Constants.PROP_PRI_ENC_SCHEMAID,Integer.toString(schemeId));
        if (extraHeader.equalsIgnoreCase(Constants.PROP_TRUE)) {
            props.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,Constants.PROP_TRUE);
        }
        else {
            props.put(""+Constants.PROP_PRI_ENC_EXTRAHEADER,Constants.PROP_FALSE);
        }
        return new VerifiableProperties(props);
    }

    private static DataplatformDecoder getDecoder(String scheme, Integer schemeId, String extraHeader) {
        return new DataplatformDecoder(generateProps(scheme,schemeId,extraHeader));
    }

    private static DataplatformEncoder getEncoder(String scheme, Integer schemeId, String extraHeader) {
        return new DataplatformEncoder(generateProps(scheme,schemeId,extraHeader));
    }



    @Before
    public void setUp() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();
        System.out.println(thisName+" -- start setup --- ");
        // Init Empty Record
        System.out.println(thisName+" -- Dpf section --- ");
        try {
            schemaDpf = new Schema.Parser().parse(new File(dpfSchemaPath));
            emptyRefDpfRecord = new GenericData.Record(schemaDpf);
            emptyRefDpfRecord.put("src", "");
            emptyRefDpfRecord.put("timestamp", 0);
            emptyRefDpfRecord.put("host_ip", "");
            emptyRefDpfRecord.put("rawdata", (ByteBuffer) ByteBuffer.wrap("".getBytes()));
        } catch (Exception ex) {
                System.out.println(thisName+" -- Failed to load user schema --- ");
                System.out.println(thisName+" "+ex);
                System.out.println("ERROR with schema: "+curDir+dpfSchemaPath);
        }

        System.out.println(thisName+" -- end setup --- ");
    }



    // Objective: test parsing is ok, we can then check extracted value (str, int)
    @Test
    public void testEncodeDecodeValueNoextra() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;
        DataplatformDecoder decoder=null;
        
        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();
        String hostIp="ae80:0:0:0:a00:27ff:fefc:b211";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", hostIp);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader OFF) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        System.out.println(thisName+" -- prep decoder (extraHeader OFF) --- ");
        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }

        System.out.println(thisName+" 1 ------------------------------------------------- ");
        assertEquals("Input / Record / check (src):", record.get("src"), src);
        assertEquals("Input / Record / check (color):", record.get("timestamp"), milliseconds);
        assertEquals("Input / Record / check (number):", record.get("host_ip"), hostIp);
        // Reminder rawdata is bytes
        ByteBuffer rd = (ByteBuffer) ((GenericRecord)record).get("rawdata");
        String tcpFieldRawdata=new String(rd.array());
        assertEquals("Input / Record / check (song):", tcpFieldRawdata, dataStr);

        try {
            if(encoder != null && decoder != null){
                byte[] serData=encoder.toBytes(record);
                Object decoded=decoder.fromBytes(serData);

                rd = (ByteBuffer) ((GenericRecord)decoded).get("rawdata");
                tcpFieldRawdata=new String(rd.array());
                String tcpFieldSrc=(String) ((GenericRecord)decoded).get("src").toString();
                long tcpFieldTimestamp=(long) ((GenericRecord)decoded).get("timestamp");
                String tcpFieldHostip=(String) ((GenericRecord)decoded).get("host_ip").toString();

                System.out.println(thisName+" - passed");
                assertEquals("Input / Record / check (src):", tcpFieldSrc, src);
                assertEquals("Input / Record / check (color):", tcpFieldTimestamp, milliseconds);
                assertEquals("Input / Record / check (number):", tcpFieldHostip, hostIp);
                assertEquals("Input / Record / check (song):", tcpFieldRawdata, dataStr);
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - failed");
            assertEquals(thisName+" Input / Output JSON string shall match:", "ok", "error");
            System.error.println(eofe);
        } 
        System.out.println(thisName+" -- end --- ");
    }


    @Test
    public void testEncodeDecodeValue_withextra() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;
        DataplatformDecoder decoder=null;

        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();
        String hostIp="ae80:0:0:0:a00:27ff:fefc:b211";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", hostIp);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader ON) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        System.out.println(thisName+" -- prep decoder (extraHeader ON) --- ");
        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }

        System.out.println(thisName+" 1 ------------------------------------------------- ");
        assertEquals("Input / Record / check (src):", record.get("src"), src);
        assertEquals("Input / Record / check (color):", record.get("timestamp"), milliseconds);
        assertEquals("Input / Record / check (number):", record.get("host_ip"), hostIp);
        // Reminder rawdata is bytes
        ByteBuffer rd = (ByteBuffer) ((GenericRecord)record).get("rawdata");
        String tcpFieldRawdata=new String(rd.array());
        assertEquals("Input / Record / check (song):", tcpFieldRawdata, dataStr);

        try {
            if(encoder != null && decoder != null){
                byte[] serData=encoder.toBytes(record);
                Object decoded=decoder.fromBytes(serData);

                rd = (ByteBuffer) ((GenericRecord)decoded).get("rawdata");
                tcpFieldRawdata=new String(rd.array());
                String tcpFieldSrc=(String) ((GenericRecord)decoded).get("src").toString();
                long tcpFieldTimestamp=(long) ((GenericRecord)decoded).get("timestamp");
                String tcpFieldHostip=(String) ((GenericRecord)decoded).get("host_ip").toString();
                //deserData=deserData.replaceAll("\\s+","");
                System.out.println(thisName+" - passed");
                assertEquals("Input / Record / check (src):", tcpFieldSrc, src);
                assertEquals("Input / Record / check (color):", tcpFieldTimestamp, milliseconds);
                assertEquals("Input / Record / check (number):", tcpFieldHostip, hostIp);
                assertEquals("Input / Record / check (song):", tcpFieldRawdata, dataStr);
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - failed");
            System.err.println(eofe);
            assertEquals(thisName+" Input / Output JSON string shall match:", "ok", "error");
        } 
        System.out.println(thisName+" -- end --- ");
    }

    @Test
    public void testEncodeDecodeValueFailure1() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;
        DataplatformDecoder decoder=null;
        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();
        String hostIp="ae80:0:0:0:a00:27ff:fefc:b211";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", hostIp);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));


        System.out.println(thisName+" -- prep encoder (extraHeader ON) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        System.out.println(thisName+" -- prep decoder (extraHeader OFF) --- ");
        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }

        try {
            if(encoder != null && decoder != null){
                byte[] serData=encoder.toBytes(record);
                Object deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed");
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode data with extra header if extra decoding not required:", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }

    @Test
    public void testEncodeDecodeValueFailure2() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;
        DataplatformDecoder decoder=null;
        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();
        String hostIp="ae80:0:0:0:a00:27ff:fefc:b211";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", hostIp);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader OFF) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        System.out.println(thisName+" -- prep decoder (extraHeader ON) --- ");
        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }
        try {
            if(encoder != null && decoder != null){
                byte[] serData=encoder.toBytes(record);
                Object deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed");
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode data with extra header if extra decoding not required:", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }

    @Test
    public void testEncodeEmptyValueFailure1() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;
        DataplatformDecoder decoder=null;
        String dataStr = "";
        String src="";
        long   milliseconds = 0;
        String hostIp="";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", hostIp);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader OFF) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }

        try {
            if(encoder != null && decoder != null){
                byte[] serData=encoder.toBytes(record);
                Object deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed "+deserData);
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode data with empty record", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }



    @Test
    public void testEncodeMissingFieldFailure1() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;

        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();
        String hostIp="ae80:0:0:0:a00:27ff:fefc:b211";

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader OFF) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        try {
            if(encoder != null){
                byte[] serData=encoder.toBytes(record);
                System.out.println(thisName+" - failed ");
                assertEquals(thisName+" encoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not encode data with missing field", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }

    @Test
    public void testEncodeBadFieldTypeFailure1() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        GenericRecord record = new GenericData.Record(schemaDpf);
        DataplatformEncoder encoder=null;

        String dataStr = "mysong-123-456-7890";
        String src="junit";
        long   milliseconds = System.currentTimeMillis();

        record.put("src", src);
        record.put("timestamp", milliseconds);
        record.put("host_ip", 101);
        record.put("rawdata", (ByteBuffer) ByteBuffer.wrap(dataStr.getBytes()));

        System.out.println(thisName+" -- prep encoder (extraHeader OFF) --- ");
        try {
            encoder = getEncoder(dpfSchemaPath,dpfSchemaId,"false");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- " +se);
        }
        try {
            if(encoder != null){
                byte[] serData=encoder.toBytes(record);
                System.out.println(thisName+" - failed ");
                assertEquals(thisName+" encoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not encode data with bad field type", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }
   


   @Test
    public void testDecodeBadBinaryData_failure1() {
        class Local {};
        String thisName = Local.class.getEnclosingMethod().getName();  
        // Create a record from the schema
        System.out.println(thisName+" -- start --- ");           
        DataplatformDecoder decoder=null;

        try {
             decoder = getDecoder(dpfSchemaPath,dpfSchemaId,"true");
        } catch (SerializationException se) {
             System.out.println(thisName+" ERROR -- prep encoder failed--- "+se);
        }

        try {
            if(decoder != null){
                byte[] serData="".getBytes();
                String deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed ");
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode bad binary data", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }

        try {
            if(decoder != null){
                byte[] serData="abaaaaaaaaaaaaaaaaaaaaaaac".getBytes();
                String deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed ");
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode bad binary data", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        try {
            if(decoder != null){
                byte[] serData="000005".getBytes();
                String deserData=(String) decoder.deserialize(serData, emptyRefDpfRecord);
                System.out.println(thisName+" - failed ");
                assertEquals(thisName+" decoding shall fail:", "SerializationException", "ok");
            }
        } catch (SerializationException eofe) {
            System.out.println(thisName+" - passed");
            assertEquals(thisName+" can not decode bad binary data", "SerializationException", "SerializationException");
            System.err.println(eofe);
        }
        System.out.println(thisName+" -- end --- ");
    }
}