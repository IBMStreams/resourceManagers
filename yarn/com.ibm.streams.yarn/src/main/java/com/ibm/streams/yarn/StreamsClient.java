//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.thrift.TException;

import com.ibm.streams.yarn.thrift.StreamsAMService.Client;
import com.ibm.streams.yarn.thrift.StreamsException;
import com.ibm.streams.yarn.thrift.ThriftClient;

/** StreamsClient that supplies StreamsAM binary to RM and also implements
 * a Thrift client.
 *
 */
public class StreamsClient {
	
	private YarnConfiguration yarnConf;
	private YarnClient streamsRM;
	private GetNewApplicationResponse streamsAM;
	private Config streamsConf;

	/** Constructor
	 * @param streamsConf Streams-YARN configuration
	 */
	public StreamsClient(Config streamsConf) {
		yarnConf = new YarnConfiguration();
		streamsRM = YarnClient.createYarnClient();
		this.streamsConf = streamsConf;
	}
	
	/** Starts the {@link YarnClient} service
	 * 
	 */
	public void start() {
		streamsRM.init(this.yarnConf);
		streamsRM.start();
	}
	
	/** Stops the {@link YarnClient} service
	 * 
	 */
	public void stop() {
		if(streamsRM != null) {
			streamsRM.stop();
		}
	}
	
	/** Creates a {@link LocalResource} to be submitted to RM.
	 * @param hdfsPath Path on HDFS
	 * @param resourceType
	 * @param hdfs HDFS handler
	 * @return {@link LocalResource}
	 * @throws IOException
	 */
	private LocalResource createLocalResource(Path hdfsPath,
			LocalResourceType resourceType, FileSystem hdfs) throws IOException {
		FileStatus status = hdfs.getFileStatus(hdfsPath);
		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setType(resourceType);
		localResource.setVisibility(LocalResourceVisibility.APPLICATION);          
		localResource.setResource(ConverterUtils.getYarnUrlFromPath(hdfsPath)); 
		localResource.setTimestamp(status.getModificationTime());
		localResource.setSize(status.getLen());
        return localResource;
	}
	
	/** Submits StreamsAM to RM. Internally wraps 
	 * this{@link #deployStreams(String, String, String)} by supplying
	 * default path for Streams binary.
	 * @param streamsAppName Name of the deployment
	 * @param yarnQueue YARN scheduling queue for the application
	 * @return {@link ApplicationId}
	 * @throws Exception
	 */
	public ApplicationId deployStreams(String streamsAppName, 
			String yarnQueue) throws Exception {
		return deployStreams(streamsAppName, yarnQueue, "default");
	}
	
	/** Submits StreamsAM to RM.
	 * @param streamsAppName Name of the deployment
	 * @param yarnQueue YARN scheduling queue for the application
	 * @param streamsPath Path to Streams binary
	 * @return {@link ApplicationId}
	 * @throws Exception
	 */
	public ApplicationId deployStreams(String streamsAppName, 
			String yarnQueue, String streamsPath) throws Exception {
		//TODO: security
		if(!streamsPath.equals("default") && !new File(streamsPath).exists()) {
			throw new Exception("Error! Path to Streams path does not exist!");
		}
		// get new application ID
		streamsAM = streamsRM.createApplication().getNewApplicationResponse();
		// set up application context for StreamsAM
		ApplicationSubmissionContext streamsAMContext = 
                Records.newRecord(ApplicationSubmissionContext.class);
        streamsAMContext.setApplicationId(streamsAM.getApplicationId());
        streamsAMContext.setApplicationName(streamsAppName);
        streamsAMContext.setQueue(yarnQueue);
        // set up container context for StreamsAM
        ContainerLaunchContext streamsAMContainer = 
        		Records.newRecord(ContainerLaunchContext.class);
        // get HDFS handler
        FileSystem hdfs = FileSystem.get(yarnConf);
        // set up local resources
        Map<String, LocalResource> localResources = 
                new HashMap<String, LocalResource>();
        // copy StreamsAM jar to HDFS
        Path hdfsJarPath = Utils.copyToHDFS(hdfs, 
        		streamsAM.getApplicationId().toString(),
        		Utils.getJarPath(StreamsAM.class),
        		"StreamsAM.jar");
        Path streamsRuntimeJarPath = Utils.copyToHDFS(hdfs, 
        		streamsAM.getApplicationId().toString(),
        		Utils.getJarPath(com.ibm.distillery.utils.DistilleryException.class),
        		"streams.runtime.jar");
        // copy Streams Instance Manager jar to HDFS
        Path streamsIMJarPath = Utils.copyToHDFS(hdfs, 
        		streamsAM.getApplicationId().toString(),
        		Utils.getJarPath(com.ibm.streams.instancemanager.exp.InstanceManagerException.class),
        		"streams.im.jar");
        // copy Thrift jar to HDFS
        Path thriftJarPath = Utils.copyToHDFS(hdfs, 
        		streamsAM.getApplicationId().toString(),
        		Utils.getJarPath(org.apache.thrift.TException.class),
        		"libthrift.jar");
        // copy Streams-YARN config file to HDFS
        Path hdfsConfigPath = Utils.copyToHDFS(hdfs, 
        		streamsAM.getApplicationId().toString(),
        		Config.PATH_CONFIG,
        		Config.CONFIG_FILE);
        if(!streamsPath.equals("default")) {
	        // copy Streams package to HDFS
	        Path hdfsStreamsPath = Utils.copyToHDFS(hdfs,
	        		streamsAM.getApplicationId().toString(),
	        		streamsPath,
	        		"Streams.zip");
	        // submit Streams package as resource
	        LocalResource streamsRsrc = createLocalResource(hdfsStreamsPath,
	        		LocalResourceType.ARCHIVE,
	        		hdfs);
	        localResources.put("Streams", streamsRsrc);  
        }
        // submit StreamsAM as resource
        LocalResource streamsAMJarRsrc = createLocalResource(hdfsJarPath,
        		LocalResourceType.FILE,
        		hdfs);
        localResources.put("StreamsAM.jar",  streamsAMJarRsrc);
        // submit runtime jar as resource
        LocalResource streamsRuntimeJarsrc = createLocalResource(streamsRuntimeJarPath,
        		LocalResourceType.FILE,
        		hdfs);
        localResources.put("streams.runtime.jar",  streamsRuntimeJarsrc);
        // submit config as resource
        LocalResource streamsConfigRsrc = createLocalResource(hdfsConfigPath,
        		LocalResourceType.FILE,
        		hdfs);
        localResources.put(Config.CONFIG_FILE,  streamsConfigRsrc);  
        // submit Streams InstanceManager jar as resource
        LocalResource streamsIMJarRsrc = createLocalResource(streamsIMJarPath,
        		LocalResourceType.FILE,
        		hdfs);
        localResources.put("streams.im.jar",  streamsIMJarRsrc);  
        // submit Thrift jar as resource
        LocalResource thriftJarRsrc = createLocalResource(thriftJarPath,
        		LocalResourceType.FILE,
        		hdfs);
        localResources.put("libthrift.jar",  thriftJarRsrc);  
        streamsAMContainer.setLocalResources(localResources);
        // start: boiler-plate code
        Map<String, String> env = new HashMap<String, String>();    
        String classPathEnv = "$CLASSPATH:./*:";    
        env.put("CLASSPATH", classPathEnv); 
        for (String path : yarnConf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
        	Apps.addToEnvironment(env, Environment.CLASSPATH.name(), path.trim());
        }        
        streamsAMContainer.setEnvironment(env);
        String command = 
                "java" +
                " com.ibm.streams.yarn.StreamsAM" + 
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"; 

        List<String> commands = new ArrayList<String>();
        commands.add(command);
        streamsAMContainer.setCommands(commands);
        // end: boiler-plate code
        
        // set up container capabilities
        Resource streamsAMContainerCapability = 
        		Records.newRecord(Resource.class);
        streamsAMContainerCapability.setMemory(streamsConf.getInt(Config.STREAMS_AM_MEM));
        streamsAMContainerCapability.setVirtualCores(streamsConf.getInt(Config.STREAMS_AM_CPU));
        streamsAMContext.setResource(streamsAMContainerCapability);
        streamsAMContext.setAMContainerSpec(streamsAMContainer);
        // submit application and return its ID to console
        streamsRM.submitApplication(streamsAMContext);
        // write app ID to config file
        streamsConf.updateString(Config.STREAMS_APP_ID, streamsAM.getApplicationId().toString());
        return streamsAM.getApplicationId();
	}
	
	/** Gets hostname for an application ID
	 * @param id
	 * @return
	 * @throws YarnException
	 * @throws IOException
	 */
	private String applicationIDToHostname(String id) throws YarnException, IOException {
		ApplicationReport report = streamsRM.getApplicationReport(ConverterUtils.toApplicationId(id));
		return report.getHost();
	}
	
	/** Main function that directly supplies StreamsAM to RM or if StreamsAM
	 * is really running, uses its Thrift client interface to manipulate it. 
	 * Exposes a command line interface for interaction.
	 * @param args Command line args supplied by streams-yarn script
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/* args[0]: command
		 * args[1]: instance ID
		 */
		try {
			Config streamsConfig = new Config();
			String appId = streamsConfig.getString(Config.STREAMS_APP_ID);
			if(args[0].equals("start")) {
				if(appId.equals("")) {
					StreamsClient streamsClient = new StreamsClient(streamsConfig);
					streamsClient.start();
					ApplicationId id = null;
					try {
						if(args.length == 2) {
							id = streamsClient.deployStreams("streams-yarn",
									"default", args[1]);
						} else {
							id = streamsClient.deployStreams("streams-yarn",
									"default");
						}
						System.out.println("Streams launched with Application ID: " + id);
					}			
					finally {
						streamsClient.stop();
					}
				} else {
					Utils.printError("Streams deployment already running with ID: " + appId);
				}
			} else {
				// check if application is running
				if(!appId.equals("")) {
					StreamsClient streamsClient = new StreamsClient(streamsConfig);
					streamsClient.start();
					ThriftClient thriftClient = new ThriftClient(streamsClient.applicationIDToHostname(appId),
							streamsConfig.getInt(Config.STREAMS_AM_PORT));
		        	thriftClient.open();
		        	Client client = thriftClient.getClient();
		        	try {
						if(args[0].equals("stop")) {			
					        try {			  
					        	// refresh appId
					        	streamsConfig.updateString(Config.STREAMS_APP_ID, "");
					        	client.stop();			        
							} catch (TException e) {
								//TODO figure out why this exception is being raised!
							}
						} else if(args[0].equals("lsinstance")) {
				        	client.lsInstance();
						} else if(args[0].equals("startinstance")) {
				        	client.startInstance(args[1], args[2], args[3], args[4], args[5]);
						} else if(args[0].equals("stopinstance")) {
				        	client.stopInstance(args[1]);
						} else if(args[0].equals("addhost")) {
				        	client.addHost(args[1], args[2]);
						} else if(args[0].equals("rmhost")) {
				        	client.removeHost(args[1],args[2]);
						}
//		        	} catch (StreamsException e) {
//		        		throw e;
		        	} finally {
		        		streamsClient.stop();
		        		thriftClient.close();
		        	}
				} else {
					Utils.printError("No Streams deployment within YARN found!");
				}
			}
		} catch (Exception e) {
    		System.err.println(e);
    		System.exit(1);
    	}
	}
}
