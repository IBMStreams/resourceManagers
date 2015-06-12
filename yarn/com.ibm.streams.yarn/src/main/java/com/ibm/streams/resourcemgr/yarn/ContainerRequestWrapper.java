//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;

import com.ibm.streams.resourcemgr.ResourceDescriptor;
import com.ibm.streams.resourcemgr.ResourceDescriptorState;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities;
import com.ibm.streams.resourcemgr.yarn.ContainerWrapper.ContainerWrapperState;

class ContainerRequestWrapper {
	private String id, domainId = null, zkConnect = null;
	private Set<String> tags = new HashSet<String>();
	private int memory = -1, cpu = -1, priority=-1;
	private ContainerRequest request ;
	private ContainerWrapper allocatedContainer = null;
	private boolean isMaster = false;
	private boolean cancelled = false;
	public ContainerRequestWrapper(String id, String domainId, String zk, int priority) {
		this.id = id;
		this.domainId = domainId;
		this.zkConnect = zk;
		this.priority = priority;
	}
	public String getId() {
		return id;
	}
	
	public String getDomainId() {
		return domainId;
	}
	public String getZkConnect() {
		return zkConnect;
	}
	public boolean isMaster() {
		return isMaster;
	}
	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}
	public int getMemory() {
		return memory;
	}
	public void setMemory(int memory) {
		this.memory = memory;
	}
	public int getCpu() {
		return cpu;
	}
	public void setCpu(int cpu) {
		this.cpu = cpu;
	}
	public Set<String> getTags() {
		return tags;
	}
	
	public int getPriority() {
		return priority;
	}
	public void cancel() {
		cancelled = true;
		if(allocatedContainer!=null)
			allocatedContainer.cancel();
	}
	public boolean isCancelled() {
		return cancelled ;
	}
	
	public void setAllocatedContainer(ContainerWrapper cw) {
		allocatedContainer = cw;
		allocatedContainer.setId(id);
		if(cancelled)
			allocatedContainer.cancel();
	}
	public ContainerWrapper getAllocatedContainer() {
		return allocatedContainer;
	}
	
	public boolean isAllocated() {
		return allocatedContainer != null;
	}
	public boolean isRunning() {
		return isAllocated() && allocatedContainer.getWrapperState() == ContainerWrapperState.RUNNING;
	}
	public ResourceDescriptor getDescriptor () {
		if(allocatedContainer != null)
			return allocatedContainer.getDescriptor();
		return StreamsAppMaster.getDescriptor(id, null);
	}
	public ResourceDescriptorState getDescriptorState () {
		if(allocatedContainer != null)
			return allocatedContainer.getDescriptorState();
		return StreamsAppMaster.getDescriptorState(false, getDescriptor());
	}
	public List<String> getStartupCommand (String installPath, String homeDir, boolean deployStreams) {
		List<String> cmd = new ArrayList<String>();
		cmd.add("export HOME=" + homeDir + ";");
		if(!deployStreams) {
			cmd.add(" source " + installPath + "/bin/streamsprofile.sh; ");
			cmd.add(" ln -s " + installPath + " StreamsLink; ");
		}
		else {
			cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents_pre; ");
			cmd.add(" ./" + StreamsYarnConstants.RES_STREAMS_BIN + "/StreamsResourceInstall/streamsresourcesetup.sh"
				+ " --install-dir ./InfoSphereStreams"
				+ " &> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/streamsinstall || exit 1;");
			cmd.add(" source ./InfoSphereStreams/*/bin/streamsprofile.sh;");
			cmd.add(" cat $PWD/InfoSphereStreams/.streams.version.dir | " +
					" xargs -I '{}' ln -s '$PWD/InfoSphereStreams/{}'  StreamsLink;");
		}
		cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents; ");
		cmd.add(" env > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/env ;");
		cmd.add(" cat launch*.sh > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/launch_context;");
		cmd.add(ResourceManagerUtilities.getStartControllerCommand(
				"$PWD/StreamsLink",
				getZkConnect(),
				getDomainId(), 
				getDescriptor(), 
				getTags(), 
				false, //false = do not fork
				isMaster()));
		
		cmd.add(" &>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/application.log");
		return cmd;
	}
	public ContainerRequest getCreateResourceRequest() throws UnknownHostException {
		if(request != null) 
			return request;
		Priority priority = Records.newRecord(Priority.class);
		
		priority.setPriority(this.priority);
		Resource res = Records.newRecord(Resource.class);
		if(memory > 0)
			res.setMemory(memory);
		if(cpu > 0)
			res.setVirtualCores(cpu);
		request =new ContainerRequest(
				res,
				null, //hosts 
				null,//racks 
				priority, 
				true//relax locality
				);
		return request;		
	}

	public boolean matches(ContainerWrapper cWrapper) {
		
		if(cWrapper == null) return false;
		if(priority != cWrapper.getContainer().getPriority().getPriority()) {
			return false;
		}
//		if(cpu > 0 && cWrapper.getContainer().getResource().getVirtualCores() != cpu) { 
//			return false;
//		}
		if( memory > 0 && cWrapper.getContainer().getResource().getMemory() < memory ) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return "ContainerRequestWrapper [id=" + id 
				+ ", priority=" + priority	
				+ ", memory=" + memory 
				+ ", cpu=" + cpu 
				+ ", isMaster=" + isMaster  
				+ ", cancelled=" + cancelled
				+ ", tags=" + tags
				+ ", request=" + request 
				+ ", allocatedContainer=" + allocatedContainer
				+ "]";
	}
}
