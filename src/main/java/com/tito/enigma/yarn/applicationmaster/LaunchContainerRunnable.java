package com.tito.enigma.yarn.applicationmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

import com.tito.enigma.yarn.util.YarnConstants;
import com.tito.enigma.yarn.worker.EnigmaLaunchContextFactory;

/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch the
 * container that will execute the shell command.
 */
public class LaunchContainerRunnable implements Runnable {

	private static final Log LOG = LogFactory.getLog(LaunchContainerRunnable.class);

	// Allocated container
	Container container;
	ApplicationMaster applicationMaster;

	/**
	 * @param lcontainer
	 *            Allocated container
	 * @param containerListener
	 *            Callback handler of the container
	 */
	public LaunchContainerRunnable(Container lcontainer, ApplicationMaster applicationMaster) {
		this.container = lcontainer;
		this.applicationMaster = applicationMaster;

	}

	@Override
	/**
	 * Connects to CM, sets up container launch context for shell command and
	 * eventually dispatches the container start request to the CM.
	 */
	public void run() {
		LOG.info("Setting up container launch container for containerid=" + container.getId());	

		ContainerLaunchContext ctx = EnigmaLaunchContextFactory.createEnigmaLaunchContext(applicationMaster.getConf(),applicationMaster.getAppAttemptID().toString(),"",YarnConstants.APP_CONTAINER_MEMORY,applicationMaster.getAllTokens());
		applicationMaster.getContainerListener().addContainer(container.getId(), container);
		applicationMaster.getNmClientAsync().startContainerAsync(container, ctx);

	}
}