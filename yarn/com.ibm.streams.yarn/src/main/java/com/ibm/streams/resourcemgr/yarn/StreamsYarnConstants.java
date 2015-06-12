//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StreamsYarnConstants {
	
	
	public static final String 
		RESOURCE_TYPE = "yarn", 
		AM_QUEUE_NAME_DEFAULT = "default", 
		AM_NAME_PREFIX = "Streams-AM-"
	;
	
	public static final long SLEEP_UNIT_MILLIS = 500;
	
	//names of jar files to be copied
	public static final String
		AM_JAR_NAME="StreamsAM.jar"
		;

	
	//names of command line args
	public static final String 
		ZK_ARG = "--zkconnect",
		TYPE_ARG = "--type",
		MANAGER_ARG = "--manager",
		INSTALL_PATH_ARG = "--install-path",
		HOME_DIR_ARG = "--home-dir",
		DEPLOY_ARG = "--deploy",
		PROP_FILE_ARG = "--properties"
	;
	public static final Set<String> DOMAIN_ARGS = 
			new HashSet<String>(Arrays.asList("--domain-id" , "-d", "--domain"));

	//am related values
	public static final int AM_MEMORY_DEFAULT = 2048, AM_CORES_DEFAULT =1;
	public static final String AM_PROPERTIES_FILE = "streams-am.properties";
	
	//deploy resources for DC and their env variables
	public static final String
		RES_STREAMS_BIN = "StreamsResourceInstall.tar",
		RES_STREAMS_BIN_NAME = "STREAMS_BIN"
	;

	// AM properties
	public static final String
		PROPS_AM_QUEUE_NAME="AM_QUEUE_NAME",
		PROPS_AM_CORES="AM_CORES",
		PROPS_AM_MEMORY="AM_MEMORY",
		PROPS_DC_CORES="DC_CORES",
		PROPS_DC_MEMORY="DC_MEMORY",
		PROPS_WAIT_SYNC = "WAIT_SYNC_SECS",
		PROPS_WAIT_ASYNC = "WAIT_ASYNC_SECS",
		PROPS_WAIT_FLEXIBLE="WAIT_FLEXIBLE_SECS",
		PROPS_WAIT_HEARTBEAT="WAIT_HEARTBEAT_SECS"
	;
	
	//Streams Tag Names
	public static final String 
		MEMORY_TAG = "memory",
		CORES_TAG = "cores"
		;
}

