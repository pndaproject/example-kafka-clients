<?php

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

$topic = "avro.log.asa";
$schema = file_get_contents('./dataplatform-raw.avsc');
$xcom = new Xcom();

// Produce a message
$kafka = new Kafka("127.0.0.1:9092",
    [
        Kafka::LOGLEVEL         => Kafka::LOG_ON,
        Kafka::CONFIRM_DELIVERY => Kafka::CONFIRM_OFF,
        Kafka::RETRY_COUNT      => 1,
        Kafka::RETRY_INTERVAL   => 25,
    ]);
var_dump($kafka->isConnected());
//get all the available partitions
$partitions = $kafka->getPartitionsForTopic($topic);
var_dump($partitions);
//use it to OPTIONALLY specify a partition to consume from
//if not, consuming IS slower. To set the partition:
//$kafka->setPartition($partitions[0]);//set to first partition
//then consume, for example, starting with the first offset, consume 20 messages
$msgSet = $kafka->consume($topic, 0, 20);//dumps array of messages
foreach ($msgSet as $msg) {
	$decode_msg = $xcom->decode($msg, $schema);
	var_dump($decode_msg);
}
?>