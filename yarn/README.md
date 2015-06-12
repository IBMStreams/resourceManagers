*Copyright (C) 2015, International Business Machines Corporation and others. All Rights Reserved.*

# Readme file for setting up Apache Hadoop YARN support for IBM® InfoSphere Streams
This Readme file describes how to install and configure Apache Hadoop YARN (Yet Another Resource Negotiator) for InfoSphere® Streams.

## Dependencies
* Apache Maven
    * To check whether Maven is installed on your system, enter which mvn.
    * To download Maven, go to the [Apache Maven website](https://maven.apache.org/).
* InfoSphere Streams
* [Apache Hadoop Version 2.7.0](https://hadoop.apache.org/), which includes Apache Hadoop YARN

## Compiling the application
Use Maven to compile the source code for the application in the InfoSphere Streams YARN package.

1. Run the following command in your project directory. This command creates a tar.gz file in the target directory.
    `mvn package`
2. Extract the contents of the tar.gz file into a directory of your choice, change to that directory, and run the application.

## Running the application

1. Ensure that InfoSphere Streams is installed and running.
2. Install and configure Apache Hadoop YARN.
    * To install YARN, install Apache Hadoop Version 2.7.0.
    * Update the following environment variables for your system:
    ```
    HADOOP_COMMON_HOME
    HADOOP_COMMON_LIB_NATIVE_DIR
    HADOOP_CONF_DIR
    HADOOP_HDFS_HOME
    HADOOP_HOME
    HADOOP_PREFIX
    HADOOP_YARN_HOME
    JAVA_HOME
    YARN_HOME
    ```
    For installation and configuration instructions, see the documentation on the Apache Hadoop website.
3. Configure YARN for InfoSphere Streams.
    * Enter `cd $YARN_HOME/etc/hadoop`.
    * Set the following properties in the yarn-site.xml file:
        * `yarn.nodemanager.vmem-check-enabled property`: The preferred setting for this property is false. This setting prevents YARN from stopping applications if the virtual memory for an application exceeds the threshold set for the application.
        * `yarn.application.classpath property`: This property must be set to the appropriate value for the operating system as defined in the yarn-default.xml file.
4. Ensure that the YARN cluster is up and running. For information about cluster setup and operation, see the documentation on the Apache Hadoop website.
5. Create an InfoSphere Streams enterprise domain and specify YARN as the external resource manager for the domain.
    * If you create the domain by using the Domain Manager, enter yarn in the External resource manager field.
    * If you create the domain by using the streamtool mkdomain command, specify `--property domain.externalResourceManager=yarn` on the command.
6. Review the following properties in the InfoSphere Streams YARN application master properties file and update as needed. This file is in the $STREAMS_INSTALL/etc/yarn directory.
    * YARN application master properties
    ```
    AM_QUEUE_NAME=default # Queue name for the application master.
    AM_CORES=2            #	Cores that are allocated to the application master.
    AM_MEMORY=2048        # Memory that is allocated to the application master.
    DC_CORES=2            #	Default number of cores to be requested for the domain controller service.
    DC_MEMORY=8192        # Default memory in bytes to be requested for the domain controller service.
    WAIT_SYNC_SECS=30     # Maximum wait time in seconds for resources to be allocated on a SYNCHRONOUS call.
    WAIT_ASYNC_SECS=5     # Maximum wait time in seconds for resources to be allocated on an ASYNCHRONOUS call.
    WAIT_FLEXIBLE_SECS=5  # Maximum wait time in seconds for resources to be allocated on a FLEXIBLE call.
    WAIT_HEARTBEAT_SECS=5 # Maximum wait time in seconds between heartbeats sent to Apache Hadoop YARN.
    ```

7. Start the YARN application master by entering the following command:
    `$STREAMS_INSTALL/bin/streams-on-yarn start --zkconnect host:port -d domain-id --deploy`

    Notes:
    * The `--zkconnect` option specifies the name of one or more host and port pairs for the configured external ZooKeeper ensemble. This value is the external ZooKeeper connection string. If the STREAMS_ZKCONNECT environment variable is set to this value, you do not need to specify the `--zkconnect` option on the command. To obtain this value, enter the `streamtool getzk -d domain-id` command.
    * The `-d` option specifies the domain identifier. If the STREAMS_DOMAIN_ID environment variable is set to this value, you do not need to specify the `-d` option on the command.
    * The `--deploy` option enables you to take advantage of the InfoSphere Streams provisioning features. When you specify this option, YARN copies and extracts the installer for each container. To avoid timeout issues, ensure that the value of the InfoSphere Streams `domain.serviceStartTimeout` property is at least 300 before you start the domain.
    To view domain properties, use the streamtool getdomainproperties command. To update properties, use the streamtool setdomainproperties command. For more information about domain properties, use the streamtool man domainproperties command.
8. Start the InfoSphere Streams domain by using the Domain Manager or the streamtool startdomain command.
9. To stop the YARN application master, enter the following command:

    `$STREAMS_INSTALL/bin/streams-on-yarn stop --zkconnect host:port -d domain-id ` 

    Notes:
    * The `--zkconnect` option specifies the name of one or more host and port pairs for the configured external ZooKeeper ensemble. This value is the external ZooKeeper connection string. If the STREAMS_ZKCONNECT environment variable is set to this value, you do not need to specify the --zkconnect option on the command. To obtain this value, enter the streamtool getzk -d domain-id command.
    * The `-d` option specifies the domain identifier. If the STREAMS_DOMAIN_ID environment variable is set to this value, you do not need to specify the -d option on the command.

