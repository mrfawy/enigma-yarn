package com.tito.enigma.yarn.client;

import java.io.IOException;
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
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.tito.enigma.yarn.util.LocalResourcesUtil;
import com.tito.enigma.yarn.util.YarnConstants;

public class ApplicationMasterLaunchContextFactory {
	private static final Log LOG = LogFactory.getLog(ApplicationMasterLaunchContextFactory.class);

	public static ContainerLaunchContext createAppMasterLaunchContext(Configuration conf, String appId,
			String engimaJarPath,String applicationMasterClassName, Map<String, String> appMasterArgs) {

		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LOG.info("Copy App Master jar from local filesystem and add to local environment");
		// Copy the application jar to the filesystem
		// Create a local resource to point to the destination jar path

		try {
			LocalResourcesUtil.addToLocalResources(conf, engimaJarPath, YarnConstants.APP_JAR, appId, localResources,
					null);
			// Set the env variables
			LOG.info("Set the environment for the application master");
			Map<String, String> env = new HashMap<String, String>();

			// add ClassPath to env variables
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
					.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
			for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

			env.put("CLASSPATH", classPathEnv.toString());
			
					
			LocalResource appJarResource = localResources.get(YarnConstants.APP_JAR);
			FileSystem fs=FileSystem.get(conf);
		    Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
		    FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
		    long hdfsAppJarLength = hdfsAppJarStatus.getLen();
		    long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

		    env.put(YarnConstants.APP_JAR, hdfsAppJarPath.toString());
		    env.put(YarnConstants.APP_JAR_TIMESTAMP, Long.toString(hdfsAppJarTimestamp));
		    env.put(YarnConstants.APP_JAR_LENGTH, Long.toString(hdfsAppJarLength));

			// Set the necessary command to execute the application master
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);

			// Set java executable command
			LOG.info("Setting up app master command");
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			// Set Xmx based on am memory size
			vargs.add("-Xmx" + YarnConstants.APP_MASTER_MEMORY + "m");
			// Set class name
			vargs.add(applicationMasterClassName);			
			vargs.add("-appMasterClass "+applicationMasterClassName);			
			
			//pass args from client
			for(String arg:appMasterArgs.keySet()){
				String value=appMasterArgs.get(arg);
				if(value!=null && !value.isEmpty()){
					vargs.add(String.format("-%s %s",arg,value));
				}
				
			}
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			LOG.info("Completed setting up app master command " + command.toString());
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());

			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null,
					null, null);
			return amContainer;
		} catch (IOException e) {

			e.printStackTrace();
		}
		return null;

	}

}
