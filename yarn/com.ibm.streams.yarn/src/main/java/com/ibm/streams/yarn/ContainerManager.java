//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.instancemanager.container.IContainer;
import com.ibm.streams.instancemanager.container.IContainer.State;
import com.ibm.streams.instancemanager.container.IContainerManager;
import com.ibm.streams.instancemanager.container.IContainerRequest;

/** Incharge of all container state. Uses {@link AMRMClientImpl} to request,
 * deploy, and release containers. 
 */
public class ContainerManager implements IContainerManager {
	private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);

	private AMRMClientImpl<ContainerRequest> streamsAMRM = null;
	private NMClient nmClient;
	private Set<Long> allocatedContainersSet;
	private  int requestCount = 0, deployedCount = 0, releasedCount = 0;
	private Map<Long, IContainer> containerMap;
	private Config config = null;
	private Queue<ContainerRequest> requestQueue;
	private List<LocalContainerRequest> pendingRequests;
	private Resource availableResources;

	/** Internal class used to maintain pending container requests. */
	private static class LocalContainerRequest {
		ContainerRequest cr = null;
		IContainerRequest containerRequest = null;
		LocalContainerRequest(ContainerRequest cr,
				IContainerRequest containerRequest) {
			this.cr = cr;
			this.containerRequest = containerRequest;
		}
	}

	/** 
	 * Constructor
	 * @param streamsAMRM Reference to AMRM service
	 * @param nmClient Reference to AMNM service
	 * @param c Reference to Streams YARN configuration
	 */
	public ContainerManager(AMRMClientImpl<ContainerRequest> streamsAMRM, NMClientImpl nmClient, Config c) {
		this.streamsAMRM = streamsAMRM;
		this.nmClient = nmClient;
		this.config = c;
		pendingRequests = new ArrayList<LocalContainerRequest>();
		requestQueue = new LinkedList<ContainerRequest>();
		allocatedContainersSet = new HashSet<Long>();
		containerMap = new HashMap<Long, IContainer>();
		availableResources = null;
	}
	
	
	/**
	 * Returns a {@link Resource}  object with the specified memory and CPU
	 * @param mem Amount of memory required
	 * @param cpu Number of virtual CPUs
	 * @return {@link Resource}
	 */
	private Resource getResource(String mem, String cpu) {
		Resource res = Records.newRecord(Resource.class);
		res.setMemory(config.getInt(mem));
		res.setVirtualCores(config.getInt(cpu));
		return res;
	}

	/**
	 * Returns a {@link ContainerRequest} object for a specified Streams 
	 * service. Resource requirements are obtained from the configuration
	 * file.
	 * @param name Name of the service
	 * @param hosts Hosts preference
	 * @return {@link ContainerRequest}
	 */
	private ContainerRequest getResourceForService(String name, String[] hosts) {
		Priority priority = Records.newRecord(Priority.class);
		Resource res = null;
		
		if(name.equals("hc")) {
			priority.setPriority(config.getInt(Config.STREAMS_HC_PRIORITY));
			res = getResource(Config.STREAMS_HC_MEM, Config.STREAMS_HC_CPU);
		}
		else if(name.equals("nsr")) {
			priority.setPriority(config.getInt(Config.STREAMS_NSR_PRIORITY));
			res = getResource(Config.STREAMS_NSR_MEM, Config.STREAMS_NSR_CPU);
		}
		else if(name.equals("sam")) {
			priority.setPriority(config.getInt(Config.STREAMS_SAM_PRIORITY));
			res = getResource(Config.STREAMS_SAM_MEM, Config.STREAMS_SAM_CPU);
		}
		else if(name.equals("srm")) {
			priority.setPriority(config.getInt(Config.STREAMS_SRM_PRIORITY));
			res = getResource(Config.STREAMS_SRM_MEM, Config.STREAMS_SRM_CPU);
		}
		else if(name.equals("sch")) {
			priority.setPriority(config.getInt(Config.STREAMS_SCH_PRIORITY));
			res = getResource(Config.STREAMS_SCH_MEM, Config.STREAMS_SCH_CPU);
		}
		else if(name.equals("aas")) {
			priority.setPriority(config.getInt(Config.STREAMS_AAS_PRIORITY));
			res = getResource(Config.STREAMS_AAS_MEM, Config.STREAMS_AAS_CPU);
		}
		else if(name.equals("sws")) {
			priority.setPriority(config.getInt(Config.STREAMS_SWS_PRIORITY));
			res = getResource(Config.STREAMS_SWS_MEM, Config.STREAMS_SWS_CPU);
		}

		ContainerRequest cr = 
				new ContainerRequest(
						res,
						hosts, null, 
						priority, false);
		return cr;
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.instancemanager.container.IContainerManager#requestContainer(com.ibm.streams.instancemanager.container.IContainerRequest)
	 */
	/**
	 * Adds a new {@link IContainerRequest}. Internally makes a call to
	 * {@link this#getResourceForService(String, String[])} and adds the 
	 * new request to the request queue as well as pending requests in the form
	 * of a {@link LocalContainerRequest}.
	 * Returns a {@link ContainerRequest} object for a specified Streams 
	 * service. Resource requirements are obtained from the configuration
	 * file.
	 * @param request {@link IContainerRequest} object representing a single
	 * container
	 */
	@Override
	public synchronized void requestContainer(IContainerRequest request) {
		//		if(requestCount > 0) throw new Exception("Already waiting for some");
		requestCount++;
		LOG.info("Requesting new container: " + requestCount);
		String [] hosts = null;
		if(request.getHosts() != null && request.getHosts().size() > 0) {
			hosts = new String[request.getHosts().size()];
			request.getHosts().toArray(hosts);
		}
		ContainerRequest cr = getResourceForService(request.getServiceType().toString(), hosts);
		pendingRequests.add(new LocalContainerRequest(cr, request));
		requestQueue.add(cr);
		LOG.info("Done Requesting new container: " + requestCount);
	}


	/* (non-Javadoc)
	 * @see com.ibm.streams.instancemanager.container.IContainerManager#deployContainer(com.ibm.streams.instancemanager.container.IContainer)
	 */
	/**
	 * Uses {@link NMClient} to deploy an {@link IContainer}. Internally makes
	 * a call to {@link IContainer#getStartupCommand#getEnviromentVariables} and
	 * {@link IContainer#getStartupCommand#getCommand} to set up the 
	 * environment and execution command for the container.
	 * @param container {@link IContainer} Container to be deployed
	 * @throws YarnException
	 * @throws IOException
	 */
	@Override
	public synchronized void deployContainer(IContainer container) throws YarnException, IOException {
		deployedCount++;
		LOG.info("Deploying container: " + deployedCount);
		ContainerLaunchContext containerContext = 
				Records.newRecord(ContainerLaunchContext.class);
		StringBuffer sb = new StringBuffer();
		for(Map.Entry<String, String> me : container.getStartupCommand().getEnviromentVariables().entrySet()) {
			sb.append("export ");
			sb.append(me.getKey());
			sb.append("=");
			sb.append(me.getValue());
			sb.append("; ");
		}
		for(String s : container.getStartupCommand().getCommand()) {
			sb.append(s);
			sb.append(" ");
		}

		LOG.info("Command: " + sb.toString());
		List<String> commands = new ArrayList<String>();
		commands.add(sb.toString());
		containerContext.setCommands(commands);
		ContainerImpl c  = (ContainerImpl)container;
		nmClient.startContainer(c.getContainer(), containerContext);
		container.setState(State.DEPLOYING, 0);
		allocatedContainersSet.remove(c.getContainer().getId().getId());
		LOG.info("Done Deploying container: " + deployedCount);
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.instancemanager.container.IContainerManager#stopContainer(com.ibm.streams.instancemanager.container.IContainer)
	 */
	/**
	 * Uses {@link NMClient} to stop an {@link IContainer}
	 * @param container {@link IContainer} Container to be stopped
	 * @throws YarnException
	 * @throws IOException
	 */
	@Override
	public void stopContainer(IContainer container) throws YarnException, IOException {
		ContainerImpl c = (ContainerImpl) container;
		nmClient.stopContainer(c.getContainer().getId(), c.getContainer().getNodeId());
	}

	/* (non-Javadoc)
	 * @see com.ibm.streams.instancemanager.container.IContainerManager#releaseContainer(com.ibm.streams.instancemanager.container.IContainer)
	 */
	/**
	 * Uses {@link NMClient} to release an allocated {@link IContainer}
	 * @param container {@link IContainer} Container to be released
	 */
	@Override
	public void releaseContainer(IContainer container) {
		streamsAMRM.releaseAssignedContainer(((ContainerImpl)container).getContainer().getId());
	}

	/** Internally used to match allocated containers with prior requests
	 * @param container Container to match
	 * @return IContainerRequest Matching request
	 */
	private IContainerRequest getMatchingRequest(Container container) {
		if(pendingRequests.size() == 0)
			return null;
		IContainerRequest ret  = null;
		int index = 0;
		LOG.debug("Comparing allocated container: " + container);
		for(;index<pendingRequests.size();index++) {
			LocalContainerRequest lcr = pendingRequests.get(index);
			if(	lcr.cr.getNodes().get(0).equals(container.getNodeId().getHost()) &&
				lcr.cr.getCapability().equals(container.getResource())) {
				LOG.debug("Container matches with pending request:  " + lcr.cr);
				break;
			}
			else {
				LOG.debug("Container does not match with pending request:  " + lcr.cr);
			}
		}
		if(index < pendingRequests.size())
			ret = pendingRequests.remove(index).containerRequest;
		return ret;
	}

	
	/** Work horse that is invoked at every heartbeat. Pings the RM and reports
	 * progress, submits new requests, and receives allocations. Note that 
	 * container deployment is exclusively orchestrated by
	 * {@link StreamsInstanceManager}. 
	 * @return
	 */
	public boolean run() {
		synchronized(this) {
			try {
				if(requestQueue.size() > 0) {
					LOG.info("Request queue size: " + requestQueue.size());
					streamsAMRM.addContainerRequest(requestQueue.remove());
				}
				AllocateResponse resp = streamsAMRM.allocate((float)1.0);
				LOG.info("Response: " + resp.toString());
				if(resp.getAMCommand() == AMCommand.AM_SHUTDOWN
						|| resp.getAMCommand() == AMCommand.AM_RESYNC) {
					// shutdown everything
					throw new Exception("Shutdown!");
				}
				// update notion of resources
				availableResources = resp.getAvailableResources();
				LOG.debug("Available resources: " + availableResources);
				LOG.info("Checking for allocated containers: " + resp.getAllocatedContainers().size());
				for(Container container : resp.getAllocatedContainers()) {
					LOG.info("Allocated Container Status: " + container);
					IContainerRequest req = getMatchingRequest(container);
					if(req != null) {
						LOG.info("Assigning container: " + container.getId() + " to request: " + req);
						IContainer cont = new ContainerImpl(container);
						req.setAssignedContainer(cont);
						containerMap.put(cont.getId(), cont);
						cont.setState(State.ASSIGNED, 0);
						allocatedContainersSet.add(new Long(container.getId().getId()));
					}
					else {
						LOG.info("Releasing extra container: " + container.getId());
						streamsAMRM.releaseAssignedContainer(container.getId());
						releasedCount++;
						LOG.debug("Number of released containers: " + releasedCount);
					}
				}

				List<IContainer> assg = new ArrayList<IContainer>(containerMap.values());
				int count = 0;
				for(IContainer cont : assg) {
					Container c = ((ContainerImpl)cont).getContainer();
					if(allocatedContainersSet.contains(c.getId().getId())) continue;
					try {
						ContainerStatus status = nmClient.getContainerStatus(c.getId(), c.getNodeId());
						LOG.info("Container: " + c.toString() + " , status: " + status);
						count++;
						switch(status.getState()) {
						case NEW:
							throw new Exception("Container in new state: " + cont);
						case RUNNING:
							cont.setState(State.RUNNING, 0);
							break;
						case COMPLETE:
							cont.setState(State.STOPPED, status.getExitStatus());
							containerMap.remove(cont.getId());
							break;
						}

					} catch(YarnException ye) {
						LOG.warn(ye.getMessage());
						cont.setState(State.STOPPED, -1);
						containerMap.remove(cont.getId());
					}
				}
				LOG.info("Assigned/Deployed Containers: " + count);
				
				if(pendingRequests.size() > 0) {
					for(LocalContainerRequest lcr : pendingRequests) {
						List<? extends Collection<ContainerRequest>> lst = streamsAMRM.getMatchingRequests(lcr.cr.getPriority(), lcr.cr.getNodes().get(0), lcr.cr.getCapability());
						for(Collection<ContainerRequest> ccr : lst) {
							LOG.info("Pending Request: " + ccr);
						}
					}
				}

			} catch (Exception e) {
				LOG.error(e.toString());
			}
			return requestQueue.size() > 0;
		}

	}

}