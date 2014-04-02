//
// # Licensed Materials - Property of IBM
// # Copyright IBM Corp. 2011, 2014
// # US Government Users Restricted Rights - Use, duplication or
// # disclosure restricted by GSA ADP Schedule Contract with
// # IBM Corp.
// 
package com.ibm.streams.yarn;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;;

/** Takes care of all configuration parameters. Internally uses XMLConfiguration. */
public class Config {
	/** Name of the configuration file*/
	public final static String CONFIG_FILE = "streams-on-yarn.xml";
	/** Path to configuration file*/
	public final static String PATH_CONFIG = System.getProperty( "user.home" ) 
			+ File.separator
			+ ".streams-on-yarn"
			+ File.separator
			+ CONFIG_FILE;
	private XMLConfiguration config;
	/** Application ID assigned to currently running StreamsAM by YARN*/
	public final static String STREAMS_APP_ID = "streams.yarn.id";
	/** Number of CPUs for StreamsAM*/
	public final static String STREAMS_AM_CPU = "streams.yarn.am.cpu";
	/** Amount of memory for StreamsAM*/
	public final static String STREAMS_AM_MEM = "streams.yarn.am.mem";
	/** Thrift server port for StreamsAM*/
	public final static String STREAMS_AM_PORT = "streams.yarn.am.port";
	/** Heartbeat interval for StreamsAM to ping RM*/
	public final static String STREAMS_AM_HEARTBEAT = "streams.yarn.am.heartbeat";
	
	public final static String STREAMS_HC_NUM = "streams.yarn.resources.hc.num";
	public final static String STREAMS_HC_CPU = "streams.yarn.resources.hc.cpu";
	public final static String STREAMS_HC_MEM = "streams.yarn.resources.hc.mem";
	public final static String STREAMS_HC_PRIORITY = "streams.yarn.resources.hc.priority";
	
	public final static String STREAMS_SAM_NUM = "streams.yarn.resources.sam.num";
	public final static String STREAMS_SAM_CPU = "streams.yarn.resources.sam.cpu";
	public final static String STREAMS_SAM_MEM = "streams.yarn.resources.sam.mem";
	public final static String STREAMS_SAM_PRIORITY = "streams.yarn.resources.sam.priority";

	public final static String STREAMS_SRM_NUM = "streams.yarn.resources.srm.num";
	public final static String STREAMS_SRM_CPU = "streams.yarn.resources.srm.cpu";
	public final static String STREAMS_SRM_MEM = "streams.yarn.resources.srm.mem";
	public final static String STREAMS_SRM_PRIORITY = "streams.yarn.resources.srm.priority";

	public final static String STREAMS_NSR_NUM = "streams.yarn.resources.nsr.num";
	public final static String STREAMS_NSR_CPU = "streams.yarn.resources.nsr.cpu";
	public final static String STREAMS_NSR_MEM = "streams.yarn.resources.nsr.mem";
	public final static String STREAMS_NSR_PRIORITY = "streams.yarn.resources.nsr.priority";

	public final static String STREAMS_SCH_NUM = "streams.yarn.resources.sch.num";
	public final static String STREAMS_SCH_CPU = "streams.yarn.resources.sch.cpu";
	public final static String STREAMS_SCH_MEM = "streams.yarn.resources.sch.mem";
	public final static String STREAMS_SCH_PRIORITY = "streams.yarn.resources.sch.priority";
	
	public final static String STREAMS_AAS_NUM = "streams.yarn.resources.aas.num";
	public final static String STREAMS_AAS_CPU = "streams.yarn.resources.aas.cpu";
	public final static String STREAMS_AAS_MEM = "streams.yarn.resources.aas.mem";
	public final static String STREAMS_AAS_PRIORITY = "streams.yarn.resources.aas.priority";

	public final static String STREAMS_SWS_NUM = "streams.yarn.resources.sws.num";
	public final static String STREAMS_SWS_CPU = "streams.yarn.resources.sws.cpu";
	public final static String STREAMS_SWS_MEM = "streams.yarn.resources.sws.mem";
	public final static String STREAMS_SWS_PRIORITY = "streams.yarn.resources.sws.priority";

	/** Operating timeout passed to {@link StreamsInstanceManager}*/
	public final static String STREAMS_INSTANCE_TIMEOUT_SECS = "streams.yarn.instance.timeout";
	public final static String STREAMS_INSTANCE_THREADPOOL_SIZE = "streams.yarn.instance.threads";
	
	public Config(String path) throws ConfigurationException {
		config = new XMLConfiguration(path);
	}
	
	public Config() throws ConfigurationException {
		this(PATH_CONFIG);
	}
	
	public String getString(String key) throws ConfigurationException {
		if(config.containsKey(key)) {
			return config.getString(key);
		} else {
			throw new ConfigurationException("Key " + key +  " not found");
		}
	}
	
	public int getInt(String key) throws ConfigurationException {
		if(config.containsKey(key)) {
			return config.getInt(key);
		} else {
			throw new ConfigurationException("Key " + key +  " not found");
		}
	}
	
	/** Updates a property and also saves it into the configuration file
	 * @param key The property to update
	 * @param value The updated value
	 * @throws ConfigurationException 
	 */
	public void updateString(String key, String value) throws ConfigurationException {
		config.setProperty(key, value);
		config.save();
	}
}
