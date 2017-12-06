/*
 * Name       : ConfigParser
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

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import org.apache.log4j.Level;
import com.cisco.shared.Constants;
import org.apache.log4j.Logger;
import java.util.Properties;
import java.io.FileInputStream;

public class ConfigParser {

	public static void parseConfig(String configFile, Properties mainConfig, Logger logger, String list) throws IOException {

        logger.info("Properties file to be used: ["+configFile+"]");
        // Load the configuration for this program
        InputStream inputConfigFile = null;
        try {

            inputConfigFile = new FileInputStream(configFile);

            // load a properties file
            mainConfig.load(inputConfigFile);
 
			logger.info("dbg list  		  ["+mainConfig.getProperty(list)+"]");
			logger.info("dbg kafka topic  ["+mainConfig.getProperty(Constants.PROP_KAFKA_TOPIC)+"]");
	        logger.info("dbg infolevel    ["+mainConfig.getProperty(Constants.PROP_INFO_LEVEL)+"]");
	        if ("ERROR".equalsIgnoreCase(mainConfig.getProperty(Constants.PROP_INFO_LEVEL))) {
	            Logger.getRootLogger().setLevel(Level.ERROR);
	        }
	        else if ("WARN".equalsIgnoreCase(mainConfig.getProperty(Constants.PROP_INFO_LEVEL))) {
	            Logger.getRootLogger().setLevel(Level.WARN);
	        }
	        else if ("INFO".equalsIgnoreCase(mainConfig.getProperty(Constants.PROP_INFO_LEVEL))) {
	            Logger.getRootLogger().setLevel(Level.INFO);
	        }
	        else if ("DEBUG".equalsIgnoreCase(mainConfig.getProperty(Constants.PROP_INFO_LEVEL))) {
	            Logger.getRootLogger().setLevel(Level.DEBUG);
	        }
	        else { // Default if no value match
	            Logger.getRootLogger().setLevel(Level.INFO);
	            logger.warn("run - fyi - properties info level not supported - default to INFO");
	        }

	        // Ensure the avro scheme file exists (avoid consuming message if we know it will fail on decoding)
	        File f = new File(mainConfig.getProperty(Constants.PROP_AVRO_SCHEMA));
	        if(f.isFile()) {
	            logger.warn("schema file exists - good news");
	        }
	        else {
	            logger.error("file ["+mainConfig.getProperty(Constants.PROP_AVRO_SCHEMA)+"] does not exists - bad news");
	        }

        } catch (IOException ex) {
        	logger.error("Example: java -jar producer-<version>.jar <configFile>"+ex.getMessage());
        	throw ex;
        } finally {
            if (inputConfigFile != null) {
                try {
                    inputConfigFile.close();
                } catch (IOException e) {
                    logger.error(e);
                    throw e;
                }
            }
        }
	}
}