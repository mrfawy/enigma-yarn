package com.tito.enigma.yarn.applicationmaster;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
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

			// non complete containers should not be here
			assert (containerStatus.getState() == ContainerState.COMPLETE);

			// increment counters for completed/failed containers
			int exitStatus = containerStatus.getExitStatus();
			if (0 != exitStatus) {
				// container failed
				if (ContainerExitStatus.ABORTED != exitStatus) {					
					applicationMaster.getCurrentPhaseManager().onContainerFailed(containerStatus);
					
				} else {
					// container was killed by framework, possibly preempted
					// we should re-try as the container was lost for some
					// reason
					applicationMaster.getCurrentPhaseManager().onContainerAborted(containerStatus);
					// we do not need to release the container as it would
					// be done
					// by the RM
				}
			} else {
				// nothing to do
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

		// ask for more containers if any failed
		int askCount = applicationMaster.getNumTotalContainers() - applicationMaster.getNumRequestedContainers().get();
		applicationMaster.getNumRequestedContainers().addAndGet(askCount);

		if (askCount > 0) {
			for (int i = 0; i < askCount; ++i) {
				ContainerRequest containerAsk = applicationMaster.setupContainerAskForRM();
				applicationMaster.getAmRMClient().addContainerRequest(containerAsk);
			}
		}

		if (applicationMaster.getNumCompletedContainers().get() == applicationMaster.getNumTotalContainers()) {
			applicationMaster.setDone(true);
		}
	}

	@Override
	public void onContainersAllocated(List<Container> allocatedContainers) {
		LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
		applicationMaster.getNumAllocatedContainers().addAndGet(allocatedContainers.size());
		for (Container allocatedContainer : allocatedContainers) {
			LOG.info(
					"Launching on a new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode="
							+ allocatedContainer.getNodeId().getHost() + ":" + allocatedContainer.getNodeId().getPort()
							+ ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
							+ ", containerResourceMemory" + allocatedContainer.getResource().getMemory()
							+ ", containerResourceVirtualCores" + allocatedContainer.getResource().getVirtualCores()
							+ ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

			applicationMaster.getCurrentPhaseManager().onContainersAllocated(allocatedContainer);
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
		// set progress to deliver to RM on next heartbeat
		float progress = (float) applicationMaster.getNumCompletedContainers().get()
				/ applicationMaster.getNumTotalContainers();
		return progress;

	}

	@Override
	public void onError(Throwable e) {
		applicationMaster.setDone(true);
		applicationMaster.getAmRMClient().stop();
	}
}
