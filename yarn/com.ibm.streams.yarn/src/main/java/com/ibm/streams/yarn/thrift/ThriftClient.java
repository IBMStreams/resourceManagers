//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn.thrift;

import java.net.UnknownHostException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.ibm.streams.yarn.thrift.StreamsAMService.Client;

/** Wrapper class for Thrift client
 *
 */
public class ThriftClient {		
	private TTransport transport;
	private TProtocol protocol;
	protected StreamsAMService.Client client;
		
	/**
	 * @param hostname Hostname of the server
	 * @param port Port of the server
	 * @throws UnknownHostException
	 */
	public ThriftClient(String hostname, int port) throws UnknownHostException {
		transport = new TFramedTransport(new TSocket(hostname, port));
	    protocol = new TBinaryProtocol(transport);	 
	    client = new StreamsAMService.Client(protocol);
	}
	
	/** Returns a handler to client
	 * @return {@link Client}
	 */
	public Client getClient() {
		return client;
	}
		
	/** Opens a thrift client connection
	 * @throws TTransportException
	 */
	public void open() throws TTransportException {
		transport.open();
	}
		
	/** Closes a thirft client connection
	 * 
	 */
	public void close() {
		transport.close();
	}
}