*Copyright (C) 2014, International Business Machines Corporation and others. All Rights Reserved.*


# Integration of IBM InfoSphere Streams with Apache YARN


## Overview

This package allows the management of IBM InfoSphere Streams instances using the YARN framework. A single *Streams Application Master* is capable of handling multiple Streams instances. Only one running instance of the Streams Application Master within a YARN instance is supported at this time. Note that the configuration of YARN and available cluster resources must be done prior to running this program. Please refer to the YARN documentation for additional information.


## Software Requirements

1. YARN: Hadoop 2.2.0
2. InfoSphere Streams: 3.2
3. Apache Maven: >=  3.0
4. Python: >= 2.6
5. [com.ibm.streams.instancemanager package:](https://www.ibm.com/developerworks/community/files/app/file/40d4ce74-f0b6-4e41-81b1-81e1b3517c09) >= 1.0.0


## Assumptions

1. This integration assumes that you already have YARN and InfoSphere Streams installed on your cluster.
2. The `$HOME/.streams` directory is assumed to be shared across all nodes on which Streams will be running. 
3. The Streams Instance Manager package is assumed to be available on all the hosts at the same location pointed by `STREAMS_IM` env.


## Help

`bin/streams-on-yarn`


## Toolchain

- To start/stop an instance and add/remove hosts

`bin/streams-on-yarn`
    
- Use `streamtool` for all other interaction


## Configuration

- Copy `conf/streams-on-yarn-sample.xml` to `$HOME/.streams-on-yarn/streams-on-yarn.xml`


## Getting Started

1. Ensure that YARN is up and running and you have set the `YARN_HOME` environment variable appropriately. 
2. Ensure that you have followed all steps mentioned in the **Configuration** section.
3. Download the Streams Instance Manager package, unzip and place it on the shared file system. Set the `STREAMS_IM` environment variable to point to the directory where the package is located. 
4. Build this project: `mvn package`
5. Start the Streams Application Master: `bin/streams-on-yarn start`
6. Create a streams instance as you would normally using `streamtool`
7. To start the instance: `bin/streams-on-yarn startinstance <instance-id>`
8. To add a host: `bin/streams-on-yarn addhost <instance-id> <hostname>`
9. To remove a host: `bin/streams-on-yarn rmhost <instance-id> <hostname>`
10. To stop the instance: `bin/streams-on-yarn stopinstance <instance-id>`

-Repeat steps 6 through 10 as desired. 

11. To stop the Streams Application Master:  `bin/streams-on-yarn stop`

All other instance management commands can be handled through `streamtool` as usual. 
