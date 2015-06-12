//
// *******************************************************************************
// * Copyright (C)2015, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.resourcemgr.yarn;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.ibm.streams.resourcemgr.ResourceManagerUtilities;
import com.ibm.streams.resourcemgr.ResourceManagerUtilities.ResourceManagerPackageType;
import com.ibm.streams.resourcemgr.ResourceServer;

public class StreamsAMClient   {

	
	private YarnConfiguration yarnConf;
	private YarnClient streamsRM;
	private GetNewApplicationResponse streamsAM;
	private Properties props = null;
	private String propsFile = null;

	public void initializeLocal(String propsFile)  throws Exception {
		this.propsFile = propsFile;
		props = new Properties();
		props.load(new FileReader(propsFile));
		yarnConf = new YarnConfiguration();
		streamsRM = YarnClient.createYarnClient();
		streamsRM.init(this.yarnConf);
		streamsRM.start();
	}
	public void cleanUp() {

		if(streamsRM != null) {
			streamsRM.stop();
		}
	}


	private LocalResource createLocalResource(Path hdfsPath,
			LocalResourceType resourceType, FileSystem hdfs, Map<String, String> env,
			String nameEnv) throws IOException {
		FileStatus status = hdfs.getFileStatus(hdfsPath);
		LocalResource localResource = Records.newRecord(LocalResource.class);
		localResource.setType(resourceType);
		localResource.setVisibility(LocalResourceVisibility.APPLICATION);
		localResource.setResource(ConverterUtils.getYarnUrlFromPath(hdfsPath)); 
		localResource.setTimestamp(status.getModificationTime());
		localResource.setSize(status.getLen());
		
		if(env != null ) {
			env.put(nameEnv, hdfsPath.toString());
			env.put(nameEnv + "_TIME", Long.toString(status.getModificationTime()));
			env.put(nameEnv + "_LEN", Long.toString(status.getLen()));
		}
		return localResource;
	}

	private void addLocalFile(FileSystem hdfs, Map<String, LocalResource> localResources, LocalResourceType fType,
			String localFile, String remoteFile,
			Map<String, String> env, String nameEnv) 
			throws IOException {
		// copy Streams package to HDFS
		Path hdfsStreamsPath = Utils.copyToHDFS(hdfs,
				streamsAM.getApplicationId().toString(),
				localFile,
				remoteFile);
		// submit Streams package as resource
		LocalResource streamsRsrc = createLocalResource(hdfsStreamsPath,
				fType,//LocalResourceType.FILE,
				hdfs, env,
				nameEnv);
		localResources.put(remoteFile, streamsRsrc);  
	}

	void createAndAddJar(FileSystem hdfs, Map<String, LocalResource> localResources, 
			Class<?> klass, String jarName) throws IOException {
		addLocalFile(hdfs, localResources, LocalResourceType.FILE, Utils.getJarPath(klass), jarName, null, null);
	}
	
	private Map<String, LocalResource> addLocalResources(String streamsInstallable, Map<String, String> env) throws IOException {
		System.out.println("Copying files to HDFS");
		// get HDFS handler
		FileSystem hdfs = FileSystem.get(yarnConf);
		// set up local resources
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		// copy required jars to HDFS
		createAndAddJar(hdfs, localResources, StreamsAppMaster.class, StreamsYarnConstants.AM_JAR_NAME);
		
		//am properties file
		addLocalFile(hdfs, localResources, LocalResourceType.FILE,
				propsFile, StreamsYarnConstants.AM_PROPERTIES_FILE, null, null);

		//copy installable files to hdfs
		if(streamsInstallable != null) {
			System.out.println("Copying Streams Installable to HDFS");
			
			addLocalFile(hdfs, localResources, LocalResourceType.ARCHIVE, streamsInstallable, 
					StreamsYarnConstants.RES_STREAMS_BIN, env,	StreamsYarnConstants.RES_STREAMS_BIN_NAME);
		}
		System.out.println("Done copying files to HDFS");
		return localResources;
	}
	
	private Resource addContainerCapabilities(Map<String, String> argsMap) {
		// set up container capabilities
		Resource streamsAMContainerCapability = 
				Records.newRecord(Resource.class);
		
		
		streamsAMContainerCapability.setMemory(
				Utils.getProperty(props, StreamsYarnConstants.PROPS_AM_MEMORY, StreamsYarnConstants.AM_MEMORY_DEFAULT));
		
		streamsAMContainerCapability.setVirtualCores(
				Utils.getProperty(props, StreamsYarnConstants.PROPS_AM_CORES, StreamsYarnConstants.AM_CORES_DEFAULT));
		return streamsAMContainerCapability;
	}

	//deploy streams app master
	private ApplicationId deployStreams(String streamsAppName, Map<String, String> argsMap) throws Exception {
		String streamsInstallable = null;
		String workingDirFile = null;
		if(argsMap.containsKey(StreamsYarnConstants.DEPLOY_ARG)) {
			System.out.println("Creating Streams Installable.");
			workingDirFile = "/tmp/streams.yarn." + (System.currentTimeMillis() / 1000);
			Utils.createDirectory(workingDirFile);
			streamsInstallable = 
					ResourceManagerUtilities.getResourceManagerPackage(
							workingDirFile, 
							ResourceManagerPackageType.BASE_PLUS_SWS_SERVICES);
			System.out.println("Created Streams Installable: " + streamsInstallable);
		}
		
		// get new application ID
		streamsAM = streamsRM.createApplication().getNewApplicationResponse();
		// set up application context for StreamsAM
		ApplicationSubmissionContext streamsAMContext = 
				Records.newRecord(ApplicationSubmissionContext.class);
		streamsAMContext.setApplicationId(streamsAM.getApplicationId());
		streamsAMContext.setApplicationName(streamsAppName);
		streamsAMContext.setQueue(
					Utils.getProperty(props, 
							StreamsYarnConstants.PROPS_AM_QUEUE_NAME, 
							StreamsYarnConstants.AM_QUEUE_NAME_DEFAULT)
					);
		// set up container context for StreamsAM
		ContainerLaunchContext streamsAMContainer = 
				Records.newRecord(ContainerLaunchContext.class);

		// start: boiler-plate code
		Map<String, String> env = new HashMap<String, String>();    
		String classPathEnv = "$CLASSPATH:./*";    

		Apps.addToEnvironment(env,  Environment.CLASSPATH.name(), classPathEnv, ApplicationConstants.CLASS_PATH_SEPARATOR);
		
		String[] classpaths =  yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
		if(classpaths == null)
			throw new StreamsYarnException("Cannot get YARN_APPLICATION_CLASSPATH. Perhaps this has not been set in the Yarn configuration?");

		for (String path : classpaths) {
			Apps.addToEnvironment(env, Environment.CLASSPATH.name(), path.trim(), ApplicationConstants.CLASS_PATH_SEPARATOR);
		}
		List<String> cmd = new ArrayList<String>();
		if(streamsInstallable == null) {
			//if no installable is specified, we assume streams is installed on all machines
			String streamsInstall = argsMap.get(StreamsYarnConstants.INSTALL_PATH_ARG);
			cmd.add(" source " + streamsInstall + "/bin/streamsprofile.sh; ");
			cmd.add(" ln -s " + streamsInstall + " StreamsLink; ");
		}
		else {
			cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents_pre; ");
			cmd.add(" $PWD/" + StreamsYarnConstants.RES_STREAMS_BIN + "/StreamsResourceInstall/streamsresourcesetup.sh"
						+ " --install-dir $PWD/InfoSphereStreams"
						+ " &> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/streamsinstall || exit 1;");
			cmd.add(" source $PWD/InfoSphereStreams/*/bin/streamsprofile.sh;");
			cmd.add(" cat $PWD/InfoSphereStreams/.streams.version.dir | " +
					" xargs -I '{}' ln -s '$PWD/InfoSphereStreams/{}'  StreamsLink;");
		}
		cmd.add(" ls -lR > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/dir_contents; ");
		cmd.add(" env > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/env;");
		cmd.add(" cat launch*.sh > " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/launch_context;");
		StringBuffer sb = new StringBuffer();
		sb.append(" $JAVA_HOME/bin/java -cp ");
		sb.append( " $CLASSPATH");
		sb.append(":$PWD/StreamsLink/lib/com.ibm.streams.resourcemgr.jar");
		sb.append(":$PWD/StreamsLink/lib/com.ibm.streams.resourcemgr.utils.jar");
		sb.append(":$PWD/StreamsLink/system/impl/lib/com.ibm.streams.platform.jar");
		sb.append(" ");
		sb.append(ResourceServer.class.getCanonicalName());
		for(Map.Entry<String, String> arg : argsMap.entrySet()) {
			sb.append(" ");
			sb.append(arg.getKey());
			sb.append(" ");
			sb.append(arg.getValue());
		}
		sb.append(" &>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/application.log");
		cmd.add(sb.toString());
		
		streamsAMContainer.setCommands(cmd);

		
		//add local resources
		streamsAMContainer.setLocalResources(addLocalResources(streamsInstallable, env));

		streamsAMContainer.setEnvironment(env);
		
		// set up container capabilities
		streamsAMContext.setResource(addContainerCapabilities(argsMap));

		streamsAMContext.setAMContainerSpec(streamsAMContainer);
		
		// submit application and return its ID to console
		streamsRM.submitApplication(streamsAMContext);
		if(workingDirFile != null) {
			try {
				Utils.deleteDirectory(workingDirFile);
			}catch(Exception e){}
		}
		return streamsAM.getApplicationId();
	}
	
	private YarnApplicationState getApplicationState(ApplicationId id) throws YarnException, IOException {
		return streamsRM.getApplicationReport(id).getYarnApplicationState();
	}
	private boolean waitForRunning(ApplicationId id) throws YarnException, IOException, InterruptedException {
		System.out.print("Waiting for application to start running.");
		for(int i=0;i<300;i++) {
			switch(getApplicationState(id)) {
			case FAILED:
			case FINISHED:
			case KILLED:
				return false;
			case RUNNING:
				return true;
			default:
				
			}
			if(i%2 == 0)
				System.out.print(".");
			Thread.sleep(1000);			
		}
		return false;
	}


	public static void main(String[] args) throws Exception {
		int cnt=0;
		int rc = 0;

		String command = args[cnt++].toLowerCase();

		Map<String, String> argsMap = new HashMap<String, String>();
		for(int i=cnt; i< args.length;i++) {
			if(args[i].equals(StreamsYarnConstants.DEPLOY_ARG)) {
				argsMap.put(args[i], "true");
			}
			else 
				argsMap.put(args[i], args[++i]);
		}
		StreamsAMClient streamsClient = new StreamsAMClient();

		try {
			if(command.equals("start")) {
				try {
					argsMap.put(StreamsYarnConstants.TYPE_ARG, "yarn");
					argsMap.put(StreamsYarnConstants.MANAGER_ARG, StreamsAppMaster.class.getCanonicalName());

					String propFile = argsMap.get(StreamsYarnConstants.PROP_FILE_ARG);
					streamsClient.initializeLocal(propFile);
					
					String domainId = null;
					for(String d : StreamsYarnConstants.DOMAIN_ARGS) {
						domainId = argsMap.get(d);
						if(domainId != null) break;
					}
					
						
					ApplicationId id = streamsClient.deployStreams(
							StreamsYarnConstants.AM_NAME_PREFIX + domainId, 
							argsMap);
					System.out.println("Streams App Master launched with Application ID: " + id);
					
					boolean ret = streamsClient.waitForRunning(id);
					if(ret) {
						System.out.println("done");
					}
					else {
						System.out.println("failed");
						System.err.println("Streams App Master failed to start. Last known state: " 
						+ streamsClient.getApplicationState(id));
						rc =1;
					}
					
				}			
				finally {
					streamsClient.cleanUp();
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
			rc = 1;
		}
		System.exit(rc);
	}
}
