//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceManagerException;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;

class ContainerWrapper {
	
	enum ContainerWrapperState {NEW, LAUNCHED, RUNNING, STOPPING, STOPPED, RELEASED}
	
	private static final Logger LOG = LoggerFactory.getLogger(ContainerWrapper.class);
	private Container container;
	private ContainerStatus status; 
	private String id;
	private ContainerWrapperState state = ContainerWrapperState.NEW;
	private StreamsAppMaster streamsAM = null;
	private boolean cancelled = false, shutdown=true;;
	
	ContainerWrapper(Container c, StreamsAppMaster streamsAM  ) {
		this.container = c;
		this.streamsAM = streamsAM;
	}
	public boolean setStatus(ContainerStatus cs) {
		ContainerStatus oldCs = status;
		this.status = cs;
		if(status.getState() ==  ContainerState.RUNNING)
			state = ContainerWrapperState.RUNNING;
		boolean changed = oldCs == null || oldCs.getState() != status.getState();
		if(changed)
			LOG.info("New Satus: " + this);
		return changed;
	}
	public Container getContainer() {
		return container;
	}
	public String getStatusString() {
		return (status == null ? "UNKNOWN" : status.getState().toString());
	}
	public ContainerWrapperState getWrapperState() {
		return state;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
	public String getHost() {
		return container.getNodeId().getHost();
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	public void cancel() {
		this.cancelled = true;		
		switch(state) {
		case RUNNING:
			stop();
			break;
		default:
			break;
		}
	}
	public boolean wasShutdown() {
		return shutdown;
	}
	public void shutdown() {
		this.shutdown = true;
	}
	public ResourceDescriptor getDescriptor () {
		return StreamsAppMaster.getDescriptor(id, getHost());
	}
	public ResourceDescriptorState getDescriptorState () {
		return StreamsAppMaster.getDescriptorState(this.state == ContainerWrapperState.RUNNING, getDescriptor());
	}
	
	public void start(List<String> startupCommand) 
			throws URISyntaxException, YarnException, IOException {
		switch(state) {
		case NEW:
			break;
		default:
			return;
		}
		
		ContainerLaunchContext containerContext = 
				Records.newRecord(ContainerLaunchContext.class);
		containerContext.setCommands(startupCommand);
		
		if( streamsAM.deployStreams()) {
			//add files we need copied over
			Map<String, LocalResource> localResources = 
					        new HashMap<String, LocalResource>();
			LOG.debug("Assigning local resources");
			streamsAM.addLocalResources(localResources, 
					LocalResourceType.ARCHIVE,
					StreamsYarnConstants.RES_STREAMS_BIN,
					StreamsYarnConstants.RES_STREAMS_BIN_NAME);
						
			containerContext.setLocalResources(localResources);
		}
		
		if(LOG.isInfoEnabled())
			LOG.info("Starting Domain Controller: " + containerContext  + ", in container: " + container);
		
		streamsAM.nmClient.startContainer(container, containerContext);
		state = ContainerWrapperState.LAUNCHED;
	}
	
	public void stop() {
		switch(state) {
		case NEW:
		case STOPPING:
		case STOPPED:
		case RELEASED:
			return; //nothing to do
		default:
			break;
		}
		
		state = ContainerWrapperState.STOPPING;
		try {
			ResourceDescriptor rd = getDescriptor();
			LOG.info("Stopping Domain Controller: " + rd);
			ResourceManagerUtilities.stopController(
					streamsAM.clientInfo.getZkConnect(),
					streamsAM.clientInfo.getDomainId(),
					rd,
					true //clean
					);
		}
		catch(ResourceManagerException rme) {
			LOG.warn("Error shutting down container: " + this, rme);
			rme.printStackTrace();
		}
		
		try {
			LOG.info("Stopping Container: " + container);
			streamsAM.nmClient.stopContainer(container.getId(), container.getNodeId());
		} catch (Exception e) {
			LOG.warn("Error stopping container: " + this, e);
		}

	}
	public void release() {
		switch(state) {
		case RELEASED:
			return; //nothing to do
		default:
			break;
		}
		LOG.info("Releasing Container: " + container);
		streamsAM.streamsAMRM.releaseAssignedContainer(container.getId());
		state = ContainerWrapperState.RELEASED;
	}
	
	public String toString () {
		return 
				"ID: " + id +
				", Container: " + (container == null ? "null" : container) + 
				", Status: " + (status == null ? "null" : status) + 
				", WrapperState: " + state
				;
	}
}

