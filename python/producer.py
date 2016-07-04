"""
Name:       Sample python code for Kafka Client
Purpose:    Producing messages to Kafka topics

Author:     PNDA team

Created:    14/12/2015

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
"""

import io
import sys, getopt, time
import datetime
import avro.schema
import avro.io
import random
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer, KeyedProducer
 
kafka = KafkaClient("ip6-localhost:9092")
producer = SimpleProducer(kafka)
 
# Path to user.avsc avro schema
schema_path="./dataplatform-raw.avsc"
 
# Kafka topic
topic = "avro.log.localtest"
schema = avro.schema.parse(open(schema_path).read())

extra=False 
loopMode=False
rangeValue=1

current_milli_time = lambda: int(round(time.time() * 1000))

try:
    opts, args = getopt.getopt(sys.argv[1:],"he:l",["extra=", "loop="])
except getopt.GetoptError:
    print('producer.py [-e true] [-l true] ')
    sys.exit(2)
for opt, arg in opts:
      if opt == '-h':
         print('producer.py [-e true] [-l true]')
         sys.exit()
      elif opt in ("-e", "--extra"):
         print("extra header requested")
         extra = True
      elif opt in ("-l", "--loop"):
         print("loop mode")
         loopMode = True
         rangeValue=1000

extrabytes = bytes('')

for i in xrange(rangeValue):
      writer = avro.io.DatumWriter(schema)
      bytes_writer = io.BytesIO()
      encoder = avro.io.BinaryEncoder(bytes_writer)
      #Prepare our msg data
      rawvarie="python-random-"+str(random.randint(10,10000))+"-loop-"+str(i)
      writer.write({"timestamp": current_milli_time(), "src": "ESC", "host_ip": "my_ipv6", "rawdata": rawvarie}, encoder)
      raw_bytes = bytes_writer.getvalue()
      if extra:
           elements = [0, 0, 0, 0, 23]
           extrabytes = bytes(bytearray(elements))
      producer.send_messages(topic, extrabytes+raw_bytes)
      if rangeValue > 1:
            time.sleep(0.5)
