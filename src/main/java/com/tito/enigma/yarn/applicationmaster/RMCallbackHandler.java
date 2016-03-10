package com.tito.enigma.yarn.applicationmaster;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
	private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);
	private ApplicationMaster applicationMaster;

	public RMCallbackHandler(ApplicationMaster applicationMaster) {
		this.applicationMaster = applicationMaster;
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> completedContainers) {
		LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
		for (ContainerStatus containerStatus : completedContainers) {
			LOG.info(applicationMaster.appAttemptID + " got container status for containerID="
					+ containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus="
					+ containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

			int exitStatus = containerStatus.getExitStatus();
			if (0 != exitStatus) {
				// container failed
				if (ContainerExitStatus.ABORTED != exitStatus) {
					applicationMaster.getCurrentPhaseManager().onContainerFailed(containerStatus);

				} else {
					// container was killed by framework, possibly preempted
					applicationMaster.getCurrentPhaseManager().onContainerAborted(containerStatus);

				}
			} else {
				// container completed successfully
				applicationMaster.getCurrentPhaseManager().onContainerCompleted(containerStatus);
				LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
			}
			try {
				applicationMaster.getTimeLinePublisher().publishContainerEndEvent(containerStatus);
			} catch (Exception e) {
				LOG.error("Container start event could not be pulished for "
						+ containerStatus.getContainerId().toString(), e);
			}
		}

	}

	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());

		for (Container allocatedContainer : allocatedContainers) {
			LOG.info(
					"Launching on a new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode="
							+ allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort()
							+ ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
							+ ", containerResourceMemory" + allocatedContainer.getResource().getMemory()
							+ ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores()
							+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

			applicationMaster.getCurrentPhaseManager().onContainerAllocated(allocatedContainer);
		}
	}

	@Override
	public void onShutdownRequest() {
		applicationMaster.setDone(true);
	}

	@Override
	public void onNodesUpdated(List<NodeReport> updatedNodes) {
	}

	@Override
	public float getProgress() {
		return applicationMaster.getProgress();

	}

	@Override
	public void onError(Throwable e) {
		applicationMaster.setDone(true);
		applicationMaster.getAmRMClient().stop();
	}
}
