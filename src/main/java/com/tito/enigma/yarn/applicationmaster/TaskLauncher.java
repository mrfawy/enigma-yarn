package com.tito.enigma.yarn.applicationmaster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.tito.enigma.config.ConfigLoader;
import com.tito.enigma.yarn.phase.PhaseManager;
import com.tito.enigma.yarn.task.Task;
import com.tito.enigma.yarn.util.YarnConstants;

/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch the
 * container that will execute the shell command.
 */
public class TaskLauncher implements Runnable {

	private static final Log LOG = LogFactory.getLog(TaskLauncher.class);

	// Allocated container
	Container container;
	PhaseManager phaseManager;
	Task task;

	public TaskLauncher(Container container, PhaseManager phaseManager, Task task) {
		super();
		this.container = container;
		this.phaseManager = phaseManager;
		this.task = task;
	}

	
	public void run() {
		LOG.info("Setting up container launch container for containerid=" + container.getId());

		ContainerLaunchContext ctx = createContainerLaunchContext();
		phaseManager.getContainerListener().addContainer(container.getId(), container);
		phaseManager.getNmClientAsync().startContainerAsync(container, ctx);

	}
	
	private ContainerLaunchContext createContainerLaunchContext() {
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LOG.info("Copy App Jar from local filesystem to the Task container");
		
		localResources.put(YarnConstants.APP_JAR, phaseManager.getAppJarResource());

		
		LOG.info("Set the environment for the Task container");
		Map<String, String> env = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : phaseManager.getConf().getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		if (task.getTaskContext().getEnvVariables() != null) {
			for (String envVar : task.getTaskContext().getEnvVariables().keySet()) {
				env.put(envVar, task.getTaskContext().getEnvVariables().get(envVar));
			}
		}

		
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		// Set java executable command
		LOG.info("Setting up Enigma Machine container command");
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		// Set Xmx based on container memory size
		vargs.add("-Xmx" + ConfigLoader.getContainerMemory() + "m");
		// Set class name
		vargs.add(task.getTaskContext().getClass().getCanonicalName());
		if (task.getTaskContext().getArgs() != null) {
			for (String arg : task.getTaskContext().getArgs().keySet()) {
				vargs.add(String.format("--%s %s", arg, task.getTaskContext().getArgs().get(arg)));
			}
		}

		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/task_"+task.getId()+".stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/task_"+task.getId()+".stderr");

		
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		LOG.info("Completed setting up Task Container command " + command.toString());
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());
		ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, env, commands, null,
				phaseManager.getAllTokens().duplicate(), null);
		return ctx;

	}
}