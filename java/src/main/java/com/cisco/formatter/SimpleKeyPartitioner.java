/*
 * Name       : SimpleKeyPartitioner
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

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;
import java.util.Random;
import java.util.regex.Pattern;
 
public class SimpleKeyPartitioner implements Partitioner {
  final static Logger logger = Logger.getLogger(SimpleKeyPartitioner.class);

  // 0.0.0.0-255.255.255.255
  private final String ipv4segment =
      "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])";
 
  // 0-65535
  private final String portsegment = ":(?:6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|" + "6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{1,3}|[0-9])";
  private final String ipv4address = "(" + ipv4segment + "\\.){3,3}" + ipv4segment;
  private final String ipv4addressWithPort = ipv4address + portsegment + "?";
  private final String ipv6segment = "[a-fA-F0-9]{1,4}";
  private final String ipv6address = "(" +
  // 1:2:3:4:5:6:7:8
      "(" + ipv6segment + ":){7,7}" + ipv6segment + "|" +
  // 1::, 1:2:3:4:5:6:7::
      "(" + ipv6segment + ":){1,7}:|" +
      // 1::8, 1:2:3:4:5:6::8, 1:2:3:4:5:6::8
      "(" + ipv6segment + ":){1,6}:" + ipv6segment + "|" +
      // 1::7:8, 1:2:3:4:5::7:8, 1:2:3:4:5::8
      "(" + ipv6segment + ":){1,5}(:" + ipv6segment + "){1,2}|" +
      // 1::6:7:8, 1:2:3:4::6:7:8, 1:2:3:4::8
      "(" + ipv6segment + ":){1,4}(:" + ipv6segment + "){1,3}|" +
      // 1::5:6:7:8, 1:2:3::5:6:7:8, 1:2:3::8
      "(" + ipv6segment + ":){1,3}(:" + ipv6segment + "){1,4}|" +
      // # 1::4:5:6:7:8, 1:2::4:5:6:7:8, 1:2::8
      "(" + ipv6segment + ":){1,2}(:" + ipv6segment + "){1,5}|" +
      // # 1::3:4:5:6:7:8, 1::3:4:5:6:7:8, 1::8
      ipv6segment + ":((:" + ipv6segment + "){1,6})|" +
      // ::2:3:4:5:6:7:8, ::2:3:4:5:6:7:8, ::8, ::
      ":((:" + ipv6segment + "){1,7}|:)|" +
      // fe80::7:8%eth0, fe80::7:8%1 (link-local IPv6 addresses with
      // zone index)
      "fe80:(:" + ipv6segment + "){0,4}%[0-9a-zA-Z]{1,}|" +
      // ::255.255.255.255, ::ffff:255.255.255.255,
      // ::ffff:0:255.255.255.255 (IPv4-mapped IPv6 addresses and
      // IPv4-translated addresses)
      "::(ffff(:0{1,4}){0,1}:){0,1}" + ipv4address + "|" +
      // 2001:db8:3:4::192.0.2.33, 64:ff9b::192.0.2.33 (IPv4-Embedded
      // IPv6 Address)
      "(" + ipv6segment + ":){1,4}:" + ipv4address + ")";
 
  private final String ipv6addressWithPort = "\\[" + ipv6address + "\\]"
      + portsegment + "?";


  
  public SimpleKeyPartitioner (VerifiableProperties props) {
  }
 
  public int partition(Object key, int a_numPartitions) {
    int partition = -1;
    String stringKey = (String) key;
    String partitionKeyValue=null;

    // Digit string
    if (stringKey.matches("\\d+")) {
          try {
               partition = Integer.parseInt(stringKey) % a_numPartitions;
               partitionKeyValue=stringKey;
          } 
          catch(NumberFormatException er) { 
              logger.warn("key ["+stringKey+"] can not be converted to integer");
          } 
    }
    else {
      if (Pattern.matches("^" + ipv4address + "$", stringKey)) {
        logger.debug("Key partition ipv4 only based");
        String[] parts = stringKey.split("\\.");
        partition = Integer.parseInt( parts[3] ) % a_numPartitions;
        partitionKeyValue=parts[3];
      }
      else  if (Pattern.matches("^" + ipv4addressWithPort + "$", stringKey)) {
        logger.debug("Key partition ipv4:port only based");
        String[] parts = stringKey.split("\\.");
        partition = Integer.parseInt( parts[3].split(":")[0] ) % a_numPartitions;
        partitionKeyValue=parts[3].split(":")[0];
      }
      else  if (Pattern.matches("^" + ipv6address + "$", stringKey)) {
        logger.debug("Key partition ipv6 only based");
        String[] parts = stringKey.split(":");
        if (parts[parts.length - 1].matches("^[0-9a-fA-F]+$")) {
           partition = Integer.parseInt( parts[parts.length - 1], 16 ) % a_numPartitions;
        }
        else {
          partition = Integer.parseInt( parts[parts.length - 1]) % a_numPartitions; 
        }
        partitionKeyValue=parts[parts.length - 1];
      }
      else if (Pattern.matches("^" + ipv6addressWithPort + "$", stringKey)) {
        logger.debug("Key partition ipv6:port only based");
        String address = stringKey.replace("[", "").replace("]", "");
        String[] parts = address.split(":");
        // Check if v4 inside
        int offset = parts[parts.length - 2].lastIndexOf('.');
        if (offset > 0) {
            try {
               partition = Integer.parseInt( parts[parts.length - 2].substring(offset+1)) % a_numPartitions;
               partitionKeyValue=parts[parts.length - 2].substring(offset+1);
            } 
            catch(NumberFormatException er) { 
              logger.warn("key ["+stringKey+"] - ipv6+port - can not be converted to integer");
            } 
        }
        else {
           // Check if block can be int or hex 
           if (parts[parts.length - 2].matches("^[0-9a-fA-F]+$")) {
            partition = Integer.parseInt( parts[parts.length - 2], 16) % a_numPartitions;
           }
           else {
             partition = Integer.parseInt( parts[parts.length - 2]) % a_numPartitions;
           }
           partitionKeyValue=parts[parts.length - 2];
        }
      }
    }

    if (partition == -1) {
      logger.warn("key ("+stringKey+") => fallback to default random partitioner");
      // Implement a basic random selection
      Random aleas = new Random();
      partition = aleas.nextInt(a_numPartitions);
    }
    
    logger.debug("==== key ("+stringKey+") => used ("+partitionKeyValue+") => Selected partition ["+String.valueOf(partition)+"]");
    return partition;
  }
 
}