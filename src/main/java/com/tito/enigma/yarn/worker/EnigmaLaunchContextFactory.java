package com.tito.enigma.yarn.worker;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.tito.enigma.yarn.util.YarnConstants;

public class EnigmaLaunchContextFactory {

	private static final Log LOG = LogFactory.getLog(EnigmaLaunchContextFactory.class);

	public static ContainerLaunchContext createEnigmaLaunchContext(Configuration conf, LocalResource appJar,
			Class mainClass, Map<String, String> args, Map<String, String> envVariables, int maxContainerMemory,
			ByteBuffer tokens) {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LOG.info("Copy App Jar from local filesystem to the Enigma Machine container");
		// Copy the application master jar to the filesystem
		// Create a local resource to point to the destination jar path

		localResources.put(YarnConstants.APP_JAR, appJar);

		// Set the env variables to be setup in the env where the
		// application

		LOG.info("Set the environment for the Enigma Machine container");
		Map<String, String> env = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		if (envVariables != null) {
			for (String envVar : envVariables.keySet()) {
				env.put(envVar, envVariables.get(envVar));
			}
		}

		// Set the necessary command to execute the application master
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		// Set java executable command
		LOG.info("Setting up Enigma Machine container command");
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		// Set Xmx based on container memory size
		vargs.add("-Xmx" + maxContainerMemory + "m");
		// Set class name
		vargs.add(mainClass.getCanonicalName());
		if (args != null) {
			for (String arg : args.keySet()) {
				vargs.add(String.format("--%s %s", arg, args.get(arg)));
			}
		}

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

	}

}
