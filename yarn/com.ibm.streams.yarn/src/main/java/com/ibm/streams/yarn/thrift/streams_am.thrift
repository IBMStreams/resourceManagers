//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
namespace java com.ibm.streams.yarn.thrift

exception StreamsException {
	1: string message;
}


service StreamsAMService {
	void stop() throws (1: StreamsException streamsException);
	
    void lsInstance();
    void startInstance(1: string instanceID, 2: string instanceOwner, 3: string streamsInstallDir, 4: string baseDir, 5: string instanceOwnerHomeDir) throws (1: StreamsException streamsException);
    void stopInstance(1: string instanceID) throws (1: StreamsException streamsException);
    void addHost(1: string instanceID, 2: string host) throws (1: StreamsException streamsException);
    void removeHost(1: string instanceID, 2: string host) throws (1: StreamsException streamsException);
}
