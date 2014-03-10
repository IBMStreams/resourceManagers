//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn.thrift;

import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.ibm.streams.yarn.StreamsAM;

/** Wrapper class for Thrift server
 *
 */
public class ThriftServer {
	
	private TNonblockingServerTransport serverTransport;
	@SuppressWarnings("rawtypes")
	private StreamsAMService.Processor processor;
    private TServer server;
    
    /** Constructor
     * @param streamsAM Handler to StreamsAM
     * @param port Port on which to launch server
     * @throws TTransportException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public ThriftServer(StreamsAM streamsAM, int port) throws TTransportException {
    	serverTransport = new TNonblockingServerSocket(port);		
		processor = new StreamsAMService.Processor(streamsAM);
		server = new THsHaServer(new THsHaServer.Args(serverTransport).
	            processor(processor));
    }
    
    /** Start server
     * 
     */
    public void serve() {
    	server.serve();
    }
    
    /** Stop server
     * 
     */
    public void stop() {
    	if(server.isServing()) {
			server.stop();
		}
    }
}