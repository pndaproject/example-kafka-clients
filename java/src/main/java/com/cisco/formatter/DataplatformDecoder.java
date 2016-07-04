/*
 * Name       : DataplatformDecoder
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

import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
import java.io.File;
import com.cisco.shared.Constants;
import kafka.utils.VerifiableProperties;
import kafka.serializer.Decoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

/**
 * The default implementation does nothing, just returns the same byte array it takes in.
 */
public class DataplatformDecoder extends AbstractSerDeser implements Decoder<Object> { //<byte[]>
    final static Logger logger = Logger.getLogger(DataplatformDecoder.class);
    public VerifiableProperties props;


    public DataplatformDecoder(VerifiableProperties props) {
        this.props = props;
        logger.debug("load config");
        this.props = props;
        this.schemaFile = props.getString(""+Constants.PROP_PRI_ENC_SCHEMA);
        this.schemaId=Integer.parseInt(props.getString(""+Constants.PROP_PRI_ENC_SCHEMAID));
        this.extraHeader=false;
        String configExtra= (String) "true";
        if ( configExtra.equalsIgnoreCase(props.getString(""+Constants.PROP_PRI_ENC_EXTRAHEADER)) ) {
                logger.debug("ADD schema header fields required");
                this.extraHeader=true;
            }
        this.schema = getSchema(this.schemaFile , this.schemaId, this.extraHeader);

        try {   // Create a schema
                emptyRefRecord = new GenericData.Record(this.schema);
                /* emptyRefRecord.put("src", "");
                emptyRefRecord.put("timestamp", "");
                emptyRefRecord.put("host_ip", "");
                emptyRefRecord.put("rawdata", "");*/
            } catch (Exception ex) {
                logger.error("The default init method for an empty record may not be valid with the schema: ", ex);
                System.exit(1);
        }

        logger.debug("config loaded");
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        logger.debug("fromBytes() start");
        if (bytes == null) {
            throw new SerializationException("Null Object - Do not expect decoding");
        }
        return deserialize(bytes,emptyRefRecord);
    }
}