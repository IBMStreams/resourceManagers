//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streams.resourcemgr.ResourceDescriptor;

abstract class ANotification {

	protected StreamsAppMaster streamsAM;
	protected List<ResourceDescriptor> list = new ArrayList<ResourceDescriptor>();
	public ANotification(StreamsAppMaster streamsAM) {
		this.streamsAM = streamsAM;
	}
	public List<ResourceDescriptor> getList() {
		return list;
	}
	
	public abstract void notifyStreams() ;
	
	@Override
	public String toString() {
		return list.toString();
	}
	
}

class NotifyAllocated extends ANotification {
	private static final Logger LOG = LoggerFactory.getLogger(NotifyAllocated.class);

	public NotifyAllocated(StreamsAppMaster streamsAM) {
		super(streamsAM);
	}

	@Override
	public void notifyStreams() {
		if(LOG.isInfoEnabled())
			LOG.info("Notifying Allocated: " + list);
		streamsAM.notifyAllocatedAndRunning(list);
		if(LOG.isInfoEnabled())
			LOG.info("Done Notifying");
	}
	
}

class NotifyRevoked extends ANotification {
	private static final Logger LOG = LoggerFactory.getLogger(NotifyRevoked.class);

	public NotifyRevoked(StreamsAppMaster streamsAM) {
		super(streamsAM);
	}

	@Override
	public void notifyStreams() {
		if(LOG.isInfoEnabled())
			LOG.info("Notifying Revoked: " + list);
		streamsAM.notifyRevoked(list);
		if(LOG.isInfoEnabled())
			LOG.info("Done Notifying");
	}
	
}

class NotifyRevokeImminent extends ANotification {
	private static final Logger LOG = LoggerFactory.getLogger(NotifyRevokeImminent.class);

	public NotifyRevokeImminent(StreamsAppMaster streamsAM) {
		super(streamsAM);
	}

	@Override
	public void notifyStreams() {
		if(LOG.isInfoEnabled())
			LOG.info("Notifying Revoke Imminent: " + list);
		streamsAM.notifyRevokeImminent(list);
		if(LOG.isInfoEnabled())
			LOG.info("Done Notifying");
	}
	
}
