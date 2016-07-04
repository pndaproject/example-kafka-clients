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

package com.cisco.shared;

public class Constants
{
    public static final String PROP_INFO_LEVEL                  = "info.level";
    public static final String PROP_SERIALIZATION               = "message.serialization";
    public static final String STR_SERIALIZATION_RAW            = "raw";
    public static final String STR_SERIALIZATION_AVRO           = "avro";

    // Specific section for CONSUMER
    // BEGIN Fields for reading propertie file
    public static final String PROP_ZK_HOST 					= "zookeeper.connect";
    public static final String PROP_ZK_SESSION_TIMEOUT 			= "zookeeper.session.timeout.ms";
    public static final String PROP_ZK_SYNC_TIME 				= "zookeeper.sync.time.ms";
    public static final String PROP_ZK_AUTO_COMMIT_INTERVAL 	= "auto.commit.interval.ms";

    public static final String PROP_KAFKA_GROUPID 				= "group.id";
    public static final String PROP_KAFKA_TOPIC 				= "topic";

    public static final String PROP_AVRO_SCHEMA				 	= "avro.schema";
    public static final String PROP_AVRO_SCHEMAID 				= "avro.schemaid";
    public static final String PROP_AVRO_EXTRA_HEADER 			= "avro.kafka.message.header";
    public static final String PROP_AVRO_DROP_MSG_EMPTY         = "avro.drop.message.empty";

    public static final String PROP_CONSUMER_THREADS 			= "consumer.threads";

    public static final String PROP_OUTPUT_HOST 				= "output.host";
    public static final String PROP_OUTPUT_PORT 			    = "output.port";
    // END Fields for reading propertie file
    // END Specific section for CONSUMER

    // Specific section for PRODUCER
    // BEGIN Fields for reading propertie file
    //list of brokers used for bootstrapping knowledge about the rest of the cluster
    public static final String PROP_MD_BROKER_LIST              = "metadata.broker.list";
    public static final String PROP_PARTITIONER_CLASS           = "partitioner.class";
    public static final String PROP_KEY_PARTITIONER_CLASS       = "key.serializer.class";
    public static final String PROP_PRODUCER_TYPE               = "producer.type";
    public static final String PROP_PRODUCER_COMPRESSION_CODEC  = "compression.codec";
    public static final String PROP_PRODUCER_MSG_ENCODER        = "serializer.class";
    public static final String PROP_PRODUCER_COMPRESSED_TOPIC   = "compressed.topic";
    public static final String PROP_PRODUCER_MD_REFRESH         = "topic.metadata.refresh.interval.ms";

    // AS: Async
    public static final String PROP_PRODUCER_AS_Q_MAX_MS        = "queue.buffering.max.ms";
    public static final String PROP_PRODUCER_AS_Q_MAX_MSG       = "queue.buffering.max.messages";
    public static final String PROP_PRODUCER_AS_Q_TIMEOUT       = "queue.enqueue.timeout.ms";
    public static final String PROP_PRODUCER_AS_Q_BATCH_MSG     = "batch.num.messages";

    public static final String STR_PRODUCER_TYPE_SYNC           = "sync";
    public static final String STR_PRODUCER_TYPE_ASYNC          = "async";
    public static final String KAFKA_DEFAULT_MSG_ENCODER        = "kafka.serializer.DefaultEncoder";
    public static final String KAFKA_DATAPLATFORM_MSG_ENCODER   = "com.cisco.formatter.DataplatformEncoder";
    public static final String KAFKA_DATAPLATFORM_MSG_DECODER   = "com.cisco.formatter.DataplatformDecoder";
    public static final String PROP_PRI_ENC_SCHEMA              = "dataplatform.private.schema";
    public static final String PROP_PRI_ENC_SCHEMAID            = "dataplatform.private.schemaId";
    public static final String PROP_PRI_ENC_EXTRAHEADER         = "dataplatform.private.extraheader";

    // END Fields for reading propertie file
    // END Specific section for PRODUCER

    // Specific properties for reading properties for message stucture carried in databus
    public static final String  PROP_PRODUCER_MSG_SRC           = "dataplatform.message.src";
    public static final String  PROP_PRODUCER_NIC_4_HOST_IP     = "dataplatform.message.host_ip.fromnic";
    public static final String  PROP_PRODUCER_FORMAT_IP         = "dataplatform.message.host_ip.format";
    public static final String  PROP_PRODUCER_IP_DEFAULT        = "dataplatform.message.host_ip.default";

    // ConfluentIO
    public static final byte TEST_MAGIC_BYTE                    = 0x0;
    public static final String  MSG_HEADER_CONFLUENT_IO         = "confluent.io";

    // Internal code config
    public static final Integer MAIN_THREAD_SLEEP_INT           = 10000;
    public static final Integer TCP_THREAD_SLEEP_INT            = 1000;

    // test
    public static final String PROP_TRUE                        = "true";
    public static final String PROP_FALSE                       = "false";

    // IP
    public static final String IP_LOCAL_V6                      = "::1";
    public static final String IP_V4                            = "v4";
    public static final String IP_V6                            = "v6";
}
