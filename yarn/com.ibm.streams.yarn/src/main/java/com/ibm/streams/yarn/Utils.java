//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
// 
package com.ibm.streams.yarn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Contains general helper functions.
 *
 */
public class Utils {
	
	/** Returns the path on the FS for a class
	 * @param _class Java class 
	 * @return Path on the FS
	 */
	public static String getClassPath(Class<?> _class) {
    	String classPath = _class.getName().replaceAll("\\.", "/") + ".class";
		return _class.getClassLoader().getResource(classPath).toString().split("!")[0];
	}
	
	/** Returns the path to the JAR for a class
	 * @param _class Java class
	 * @return Path to JAR on FS
	 */
	public static String getJarPath(Class<?> _class) {
    	return getClassPath(_class).substring(9);
	}
	
	/** Returns the hostname of the current host
	 * @return Hostname
	 * @throws UnknownHostException
	 */
	public static String getHostName() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostName();
	}
	
	/** Deprecated
	 * @param command
	 * @return Process object
	 * @throws IOException
	 */
	public static Process launchProcess(String[] command) throws IOException {
		ProcessBuilder processBuilder = new ProcessBuilder(command);
	    return processBuilder.start();
	}

	/** Deprecated
	 * @param command
	 * @return Output of process
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static String launchProcessAndGetOutput(String[] command) throws IOException, InterruptedException {
	    Process process = launchProcess(command);
	    process.waitFor();
	    return(inputStreamToString(process.getInputStream()));
	}
	
	/** Deprecated
	 * @param input
	 * @return String form of input stream
	 * @throws IOException
	 */
	public static String inputStreamToString(InputStream input) throws IOException {		 
		if (input != null) {
			StringWriter writer = new StringWriter();		 
			char[] buffer = new char[1024];
			try {
				Reader reader = new BufferedReader(new InputStreamReader(input));
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				input.close();
			}
			return writer.toString().trim();
		}
		else {
			return "";
		}
	}
	
	/** Returns the path on the HDFS for a particular application
	 * @param applicationID
	 * @param name
	 * @return HDFS path for application
	 */
	public static String getHDFSPath(String applicationID, String name) {
		return  applicationID + Path.SEPARATOR + name;
	}
	
	/** Copies file from local FS to the HDFS
	 * @param hdfs HDFS handler
	 * @param applicationID
	 * @param localPath Path on the local FS
	 * @param name Name of the file
	 * @return HDFS {@link Path}
	 * @throws IOException
	 */
	public static Path copyToHDFS(FileSystem hdfs, String applicationID, 
			String localPath, String name) throws IOException {
        Path hdfsPath = new Path(hdfs.getHomeDirectory(), Path.SEPARATOR
        		+ getHDFSPath(applicationID, name));
        hdfs.copyFromLocalFile(new Path(localPath), hdfsPath);
        return hdfsPath;
	}
	
	/** Prints an error message to stdout
	 * @param msg Message to be printed
	 */
	public static void printError(String msg) {
		System.out.println("Error! " + msg);
	}
}