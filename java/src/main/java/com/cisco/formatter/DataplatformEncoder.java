/*
 * Name       : DataplatformEncoder
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
import com.cisco.shared.Constants;
import kafka.utils.VerifiableProperties;
import kafka.serializer.Encoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

/**
 * The default implementation is a no-op, it just returns the same array it takes in
 */
public class DataplatformEncoder extends AbstractSerDeser implements Encoder<Object> { //<byte[]>
    final static Logger logger = Logger.getLogger(DataplatformEncoder.class);
    public VerifiableProperties props;

    public DataplatformEncoder(VerifiableProperties props) {
        logger.debug("load config");
        this.props = props;
        this.schemaFile = props.getString(""+Constants.PROP_PRI_ENC_SCHEMA);
        this.schemaId=Integer.parseInt(props.getString(""+Constants.PROP_PRI_ENC_SCHEMAID));
        this.extraHeader=false;
        String configExtra= (String) "true";
        if ( configExtra.equalsIgnoreCase(props.getString(""+Constants.PROP_PRI_ENC_EXTRAHEADER)) ) {
                logger.info("DataplatformEncoder: extraHeader TRUE");
                this.extraHeader=true;
            }
        else {
                logger.info("DataplatformEncoder: extraHeader FALSE");
        }
        this.schema = getSchema(this.schemaFile , this.schemaId, this.extraHeader);
        logger.debug("config loaded");
    }



    @Override
    public byte[] toBytes(Object obj) { //byte[] bytes
        logger.debug("toBytes() start");
        if (obj == null) {
            throw new SerializationException("Null Object - Do not expect encoding");
        }
        if (obj instanceof GenericRecord) {
            if (this.extraHeader == true) {
              logger.debug("toBytes(): extraHeader read as true");
            }
            else {
              logger.debug("toBytes(): extraHeader read as false");
            }
            return serializeImpl((GenericRecord) obj);
        }
        else {
            throw new SerializationException("Object is expected to be an instance of GenericRecord");
        }
    } // end toBytes()

}