package com.tito.enigma.yarn.worker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.tito.enigma.machine.EnigmaMachine;

public class EnigmaLaunchContextFactory {

	private static final Log LOG = LogFactory.getLog(EnigmaLaunchContextFactory.class);
	
	private static final String engimaJar = "enigma-machine.jar";
	private static final String appName="engima-yarn";

	private Configuration conf;	
	private String appId;

	public EnigmaLaunchContextFactory(Configuration conf,String appId) {
		this.conf = conf;
		this.appId=appId;		
	}

	public ContainerLaunchContext createEnigmaLaunchContext(String engimaJarPath,String maxContainerMemory, ByteBuffer tokens) {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LOG.info("Copy App Jar from local filesystem to the Enigma Machine container");
		// Copy the application master jar to the filesystem
		// Create a local resource to point to the destination jar path
		
		try {
			addToLocalResources(engimaJarPath, engimaJar, appId, localResources);
			// Set the env variables to be setup in the env where the application

			LOG.info("Set the environment for the Enigma Machine container");
			Map<String, String> env = new HashMap<String, String>();

			// put env variables 
			//env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
			
			// Add AppMaster.jar location to classpath		
			// For now setting all required classpaths including
			// the classpath to "." for the application jar
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
					.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
			env.put("CLASSPATH", classPathEnv.toString());

			// Set the necessary command to execute the application master
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);

			// Set java executable command
			LOG.info("Setting up Enigma Machine container command");
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			// Set Xmx based on container memory size
			vargs.add("-Xmx" + maxContainerMemory + "m");
			// Set class name
			vargs.add(EnigmaMachine.class.getCanonicalName());
			// Set params for Engima Machine Container
			vargs.add("--length " + String.valueOf(100));	

			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/EnigmaMachine.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/EnigmaMachine.stderr");

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			LOG.info("Completed setting up Enegma Container command " + command.toString());
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());
			ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, env, commands, null,
					tokens.duplicate(), null);
			return ctx;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private void addToLocalResources(String fileSrcPath, String fileDstPath, String appId,
			Map<String, LocalResource> localResources) throws IOException {
		
		FileSystem fs=FileSystem.get(conf);
		String suffix = appName + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);		
		fs.copyFromLocalFile(new Path(fileSrcPath), dst);		
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
				LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
				scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}
}
