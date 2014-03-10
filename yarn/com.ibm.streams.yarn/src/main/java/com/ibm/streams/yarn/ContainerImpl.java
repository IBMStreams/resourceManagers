//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn;

import org.apache.hadoop.yarn.api.records.Container;

/** Generalizes the YARN notion of a container to conform to the container 
 * interface exposed by {@link StreamsInstanceManager}
 */
class ContainerImpl extends com.ibm.streams.instancemanager.container.Container {

	private Container c = null;

	public ContainerImpl(Container c) {
		super(c.getId().getId(), c.getNodeId().getHost());
		this.c = c;
	}
	
	public Container getContainer() {
		return c;
	}
	
}
