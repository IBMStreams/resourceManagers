//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

public class StreamsYarnException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public StreamsYarnException(String msg) {
		super(msg);
	}
	public StreamsYarnException(String msg, Throwable t) {
		super(msg, t);
	}

}
