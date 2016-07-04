/*
 * Name       : IpUtil
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

import java.net.*;
import java.util.*;
import org.apache.log4j.Logger;
import com.cisco.shared.Constants;

// Utility for selecting a target IP address value retrieval (by NIC, format)
public class IpUtil  {
    final static Logger logger = Logger.getLogger(IpUtil.class);

    public String getIpPerNic(String nicName, String ipVersion) throws UnknownHostException {
        String resultStr=null;

	    Enumeration<NetworkInterface> interfaces = null;
	    try {
	        interfaces = NetworkInterface.getNetworkInterfaces();
	    } catch (SocketException se) {
	        throw new UnknownHostException("failed to list network interfaces " + se);
	    }

	    try {
	        InetAddress localhostv4 = InetAddress.getLocalHost();
	        Inet6Address localhostv6 = (Inet6Address) InetAddress.getByName(Constants.IP_LOCAL_V6);
	    } catch (UnknownHostException uhe) {
	        throw new UnknownHostException("failed to get local v4/v6 Inet addresses " + uhe);
	    }

	    while (interfaces.hasMoreElements()) {
	        NetworkInterface ifc = interfaces.nextElement();
	        String currentNicName=ifc.getDisplayName();
	        logger.debug("-- NIC : " + currentNicName);

	        Enumeration<InetAddress> addressesOfAnInterface = ifc.getInetAddresses();
	        // Check this is our target i/f
	        if (currentNicName.equalsIgnoreCase(nicName)) {
	        	while (addressesOfAnInterface.hasMoreElements()) {

		            InetAddress address = addressesOfAnInterface.nextElement();

	                if (address.toString().contains(":") && ipVersion.equals(Constants.IP_V6)) {
	                	Inet6Address address6 = (Inet6Address) address;
	                	if (address6.toString().contains("%")) {
	                	 String[] parts = address6.getHostAddress().split("%");
	                     logger.debug("FOUND v6 ADDRESS ON NIC: [" + parts[0]+"] scopeId ["+parts[1]+"]");
	                     return parts[0];
	                 	}
	                 	else {
	                     logger.debug("FOUND v6 ADDRESS ON NIC: [" + address6.getHostAddress()+"]");
	                 	}

	                } else {
		            	if (!address.toString().contains(":") && ipVersion.equals(Constants.IP_V4)) {
		                	logger.debug("FOUND v4 ADDRESS ON NIC: [" + address.getHostAddress()+"]");
		                	return address.getHostAddress();
		            	}
		            	else {
		            		logger.debug("Ignored address: [" + address.getHostAddress()+"]");
		            	}
		        	}
		        } //End while  addresses
		    }
		    // end if nicName equals
		    else {
		        	logger.debug("-- NIC : ignored");
		    }
	    } // en,d while Nic I/F
	    // When we are there, houston, we have a problem
	    // Assign a default one: no. Let the requester manages default value

	    return null;
	} // end getIpPerNic
}