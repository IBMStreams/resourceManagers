//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.instancemanager.StreamsInstanceManager;
import com.ibm.streams.yarn.thrift.StreamsAMService;
import com.ibm.streams.yarn.thrift.StreamsException;
import com.ibm.streams.yarn.thrift.ThriftServer;

/** Streams ApplicationMaster that coordinates with the RM and exposes a 
 * Thrift interface for client interaction. Maintains state for running
 * Streams instances by keeping one {@link StreamsInstanceManager} per instance
 *
 */
public class StreamsAM implements StreamsAMService.Iface {	
	private static final Logger LOG = LoggerFactory.getLogger(StreamsAM.class);
	private ApplicationAttemptId attemptId;
	private AMRMClientImpl<ContainerRequest> streamsAMRM;
	private YarnConfiguration yarnConf;
	private Map<String, StreamsInstanceManager> instances;	
	private ThriftServer thriftServer;
	private Config streamsConf;
	private NMClientImpl nmClient;
	private ContainerManager manager = null;
	long timeoutMillis = 5*60 * 1000;

	/** Constructor
	 * @param attemptId Application attempt ID assigned by YARN
	 * @param conf Streams-YARN configuration
	 * @throws TTransportException
	 */
	public StreamsAM(ApplicationAttemptId attemptId, Config conf) throws TTransportException {
		this.attemptId = attemptId;
		streamsConf = conf;
		yarnConf = new YarnConfiguration();
		streamsAMRM = new AMRMClientImpl<ContainerRequest>();
		instances = new LinkedHashMap<String, StreamsInstanceManager>();
		thriftServer = new ThriftServer(this, conf.getInt(Config.STREAMS_AM_PORT));
		nmClient = (NMClientImpl)NMClient.createNMClient();	
		manager = new ContainerManager(streamsAMRM, nmClient, conf);
		timeoutMillis = conf.getInt(Config.STREAMS_INSTANCE_TIMEOUT_SECS) * 1000;
	}

	private void startServer() {
		LOG.info("Starting Thirft Server");
		thriftServer.serve();
	}

	private void stopServer() {
		LOG.info("Stopping Thirft Server");
		thriftServer.stop();
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.yarn.thrift.StreamsAMService.Iface#stop()
	 */
	/** Stops Thrift server, YARN services, and running Streams instances
	 * @throws StreamsException
	 */
	@Override
	public synchronized void stop() throws StreamsException {
		LOG.info("Stopping StreamsAM");
		stopServer();
		// Also remove all instances
		stopInstances();
		nmClient.stop();
	}

	private void exit(int code) {
		streamsAMRM.stop();
		LOG.info("Exiting StreamsAM");
		System.exit(code);
	}

	public void stopAndExitNormal() {
		try {
			stop();
		} catch (StreamsException e) {
			LOG.error("StreamsAM Exception: " + e.getMessage());
		}
		exit(0);
	}

	public void stopAndExitError() {
		try {
			stop();
		} catch (StreamsException e) {
			LOG.error("StreamsAM Exception: " + e.getMessage());
		}
		exit(-1);
	}

	/** Starts all services as well as Thrift server
	 * 
	 */
	public void start() {
		LOG.info("Starting StreamsAM");
		nmClient.init(this.yarnConf);
		nmClient.start();
		streamsAMRM.init(this.yarnConf);
		streamsAMRM.start();
	}

	public String toString() {
		return "AttemptID: " + this.attemptId;
	}

	/** Registers the StreamsAM with the RM
	 * @return {@link RegisterApplicationMasterResponse}
	 * @throws YarnException
	 * @throws IOException
	 */
	public RegisterApplicationMasterResponse registerApplicationMaster() throws YarnException, IOException {
		LOG.info("Registering StreamsAM with RM");
		String hostname = Utils.getHostName();
		return streamsAMRM.registerApplicationMaster(hostname, streamsConf.getInt(Config.STREAMS_AM_PORT), null);
	}

	/** Unregisters the StreamAM with the RM
	 * @param status Final status of the StreamsAM
	 * @param appString 
	 * @throws YarnException
	 * @throws IOException
	 */
	public void unregisterApplicationMaster(FinalApplicationStatus status, String appString) throws YarnException, IOException {
		LOG.info("Unregistering StreamsAM with RM");
		streamsAMRM.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, appString, null);
	}

	public STATE getState() {
		return streamsAMRM.getServiceState();
	}

	private boolean processAllocation(AllocateResponse response) throws YarnException, IOException {
		LOG.info(getState().toString());
		boolean runAgain = false;
		try {
			runAgain = manager.run();
		} catch(Exception e) {
			LOG.error("Told to shutdown by RM");
			stopAndExitError();
		}
		return runAgain;		
	}

	/** TimerTask that is invoked at every heartbeat interval. Used to ping
	 * the RM. 
	 * 
	 */
	private TimerTask mainLoopTimerTask = new TimerTask(){
		@Override
		public void run() {
			LOG.info("Sending heartbeat to RM");			
			try {
				boolean runAgain = true;
				while(runAgain)
					runAgain = processAllocation(null);

			} catch (Exception e) {
				LOG.error("StreamsAM Exception: " + e.getMessage());
			} 				
		}
	};

	/** Launches the main application logic loop 
	 * 
	 */
	private void startMainLoop() {
		LOG.info("Starting main StreamsAM loop");
		try {       
			Timer mainLoopTimer = new Timer();
			mainLoopTimer.scheduleAtFixedRate(mainLoopTimerTask, 0, 
					streamsConf.getInt(Config.STREAMS_AM_HEARTBEAT));
			LOG.info("StreamsAM reporting for duty");    		
			startServer();
		} catch (Exception e) {
			// something terrible must've happened!
			LOG.error("StreamsAM Exception: " + e.getMessage());
			stopAndExitError();
		} 
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.yarn.thrift.StreamsAMService.Iface#lsInstance()
	 */
	/** Lists running Streams Instances
	 * 
	 */
	@Override
	public synchronized void lsInstance() throws TException {
		if(instances.isEmpty()) {
			LOG.info("No instances exist");
		} else {
			LOG.info("Running instances:");
			Iterator<String> iterator = instances.keySet().iterator();
			while(iterator.hasNext()){
				LOG.info(iterator.next());
			}
		}
	}

	/** Starts a Streams instance by creating a new 
	 * {@link StreamsInstanceManager}
	 * @param instanceID The user specified instance ID
	 * @param instanceOwner The user specified instance owner
	 * @param streamsInstallDir Path to Streams installation
	 * @param baseDir Path to StreamsInstanceManager directory
	 * @param instanceOwnerHomeDir Path to instance owner's home directory
	 * @throws StreamsException
	 * @throws TException
	 * 
	 */
	@Override
	public synchronized void startInstance(String instanceID, 
			String instanceOwner, String streamsInstallDir, String baseDir, 
			String instanceOwnerHomeDir) throws StreamsException,
			TException {
		// check if instance actually exists
		String sep = System.getProperty("file.separator");
		String instancePath = instanceOwnerHomeDir + sep + ".streams" + sep 
				+ "instances" + sep + instanceID + "@" + instanceOwner;
		if(new File(instancePath).exists()) {
			LOG.info("Starting instance: " + instanceID);
			if(!instances.containsKey(instanceID)) {
				try {
					instances.put(
							instanceID, 
							new StreamsInstanceManager(
									instanceID,  
									instanceOwner,  
									streamsInstallDir,  
									baseDir, 
									instanceOwnerHomeDir, 
									manager,
									timeoutMillis)
							);
				} catch (Exception e) {
					throw new StreamsException(e.getMessage());
				}
			}
			if(instances.containsKey(instanceID)) {
				try {
					instances.get(instanceID).startInstance();
				} catch (Exception e) {
					throw new StreamsException(e.toString());
				}
			} else {
				LOG.info("Error! Instance ID " + instanceID + " not found");
			}
		} else {
			String msg = "Instance " + instanceID + " does not exist. Create"
					+ " using streamtool!";
			LOG.error(msg);
			throw new StreamsException(msg);
		}
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.yarn.thrift.StreamsAMService.Iface#stopInstance(java.lang.String)
	 */
	/** Stops a Streams instance
	 * @param instanceID The user specified instance ID
	 * @throws StreamsException
	 * 
	 */
	@Override
	public synchronized void stopInstance(String instanceID) throws StreamsException {
		LOG.info("Stopping instance: " + instanceID);
		if(instances.containsKey(instanceID)) {
			StreamsInstanceManager instance = instances.get(instanceID);
			if(instance.isRunning()) {
				try {
					instance.stopInstance(true);
				} catch (Exception e) {
					throw new StreamsException(e.toString());
				}
			} else {
				LOG.info("Instance ID " + instanceID + " is not running");
			}
		} else {
			LOG.info("Error! Instance ID " + instanceID + " not found");
		}

	}

	/** Stops all running Streams instances
	 * @throws StreamsException
	 * 
	 */
	public synchronized void stopInstances() throws StreamsException {
		Iterator<String> iterator = instances.keySet().iterator();
		while(iterator.hasNext()){
			stopInstance(iterator.next());
		}
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.yarn.thrift.StreamsAMService.Iface#addHost(java.lang.String, java.lang.String)
	 */
	/** Adds a new host controller to a particular instance
	 * @param instanceID The user specified instance ID
	 * @param host The hostname on which to request a container for the HC
	 * @throws StreamsException
	 * @throws TException
	 * 
	 */
	@Override
	public synchronized void addHost(String instanceID, String host)
			throws StreamsException, TException {
		LOG.info("Adding host, instance: " + instanceID + ", host: " + host);
		if(instances.containsKey(instanceID)) {
			StreamsInstanceManager instance = instances.get(instanceID);
			if(instance.isRunning()) {
				try {
					instance.addHost(host, Arrays.asList("hc"));
				} catch (Exception e) {
					LOG.error("Operation Failed", e);
					throw new StreamsException(e.toString());
				}
			} else {
				LOG.info("Instance ID " + instanceID + " is not running");
			}
		} else {
			LOG.info("Error! Instance ID " + instanceID + " not found");
		}
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.yarn.thrift.StreamsAMService.Iface#removeHost(java.lang.String, java.lang.String)
	 */
	/** Remove a host controller from a particular instance
	 * @param instanceID The user specified instance ID
	 * @param host The hostname on which to release HC and its container
	 * @throws StreamsException
	 * @throws TException
	 * 
	 */
	@Override
	public synchronized void removeHost(String instanceID, String host)
			throws StreamsException, TException {
		LOG.info("Removing host, instance: " + instanceID + ", host: " + host);
		if(instances.containsKey(instanceID)) {
			StreamsInstanceManager instance = instances.get(instanceID);
			if(instance.isRunning()) {
				try {
					instance.removeHost(host, true);
				} catch (Exception e) {
					throw new StreamsException(e.toString());
				}
			} else {
				LOG.info("Instance ID " + instanceID + " is not running");
			}
		} else {
			LOG.info("Error! Instance ID " + instanceID + " not found");
		}

	}

	/** Main function that is invoked by the YARN Applications Manager.
	 * Creats a new StreamsAM and registers it with the RM.
	 * @param args Command line args passed by YARN
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Map<String, String> envs = System.getenv();
		// start: boiler-plate code
		String containerIdString = 
				envs.get(ApplicationConstants.Environment.CONTAINER_ID.name());
		if (containerIdString == null) {
			throw new IllegalArgumentException(
					"ContainerId not set in the environment");
		}
		ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
		ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
		// end: boiler-plate code
		StreamsAM streamsAM = new StreamsAM(appAttemptID, new Config(Config.CONFIG_FILE));
		streamsAM.start();
		try {
			RegisterApplicationMasterResponse response = streamsAM.registerApplicationMaster();
			// start main loop
			LOG.info(streamsAM.toString());
			LOG.info(response.toString());
			streamsAM.startMainLoop();
			// tell RM that we're done	
			streamsAM.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Done");	    	
		} finally {
			// exit
			streamsAM.exit(0);
		}
	}	
}