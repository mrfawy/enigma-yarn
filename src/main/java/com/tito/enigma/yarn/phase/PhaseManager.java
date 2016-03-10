package com.tito.enigma.yarn.phase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import com.tito.enigma.config.ConfigLoader;
import com.tito.enigma.yarn.applicationmaster.ApplicationMaster;
import com.tito.enigma.yarn.applicationmaster.NMCallbackHandler;
import com.tito.enigma.yarn.applicationmaster.TaskLauncher;
import com.tito.enigma.yarn.task.Task;
import com.tito.enigma.yarn.task.TaskStatus;

/**
 * Phase Managers handles running list of tasks to run in parallel until
 * completion
 * 
 * @author mabdelra
 *
 */
public abstract class PhaseManager {

	private static final Log LOG = LogFactory.getLog(PhaseManager.class);

	private ApplicationMaster applicationMaster;

	private Phase phase;

	protected Queue<Task> pendingTasks = new LinkedList<>();
	protected Queue<Task> runningTasks = new LinkedList<>();
	protected Queue<Task> completedTasks = new LinkedList<>();

	protected Map<ContainerId, Task> containerTaskMatrix = new HashMap<>();

	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();

	private List<Thread> launchThreads = new ArrayList<Thread>();

	public PhaseManager(ApplicationMaster appMaster, Phase phase) {
		this.applicationMaster = appMaster;
		this.phase = phase;
	}

	public Configuration getConf() {
		return applicationMaster.getConf();
	}

	public LocalResource getAppJarResource() {
		return applicationMaster.getAppJarResource();
	}

	public ByteBuffer getAllTokens() {
		return applicationMaster.getAllTokens();
	}

	public NMCallbackHandler getContainerListener() {
		return applicationMaster.getContainerListener();
	}

	public NMClientAsync getNmClientAsync() {
		return applicationMaster.getNmClientAsync();
	}

	public AMRMClientAsync getAmRMClient() {
		return applicationMaster.getAmRMClient();
	}

	public void RegisterTask(Task task) {
		phase.getTaskList().add(task);
		task.setStatus(TaskStatus.PENDING);
		this.pendingTasks.add(task);
	}

	public void start() {
		if (!checkDependencies()) {
			throw new RuntimeException("Dependencies Check is not met , aborting phase");
		}
		int requiredCotainers = pendingTasks.size();
		sendRequestForContainers(requiredCotainers);
	}

	private void sendRequestForContainers(int n) {

		for (int i = 0; i < n; i++) {
			Priority pri = Priority.newInstance(ConfigLoader.getPriority());
			Resource capability = Resource.newInstance(ConfigLoader.getContainerMemory(),
					ConfigLoader.getContainerVCores());
			ContainerRequest containerRequest = new ContainerRequest(capability, null, null, pri);
			LOG.info("Requested container ask: " + containerRequest.toString());
			getAmRMClient().addContainerRequest(containerRequest);
			numRequestedContainers.incrementAndGet();
		}

	}

	public void onContainersAllocated(Container allocatedContainer) {
		LOG.info("Processing Allocated Containers :" + allocatedContainer.getId());
		numAllocatedContainers.incrementAndGet();

		if (!pendingTasks.isEmpty()) {
			Task task = pendingTasks.poll();
			task.setAssignedContainerId(allocatedContainer.getId());
			task.setStatus(TaskStatus.RUNNING);
			runningTasks.add(task);
			containerTaskMatrix.put(allocatedContainer.getId(), task);
			TaskLauncher runnableLaunchContainer = new TaskLauncher(allocatedContainer, this, task);
			Thread launchThread = new Thread(runnableLaunchContainer);
			launchThreads.add(launchThread);
			launchThread.start();
		}

	}

	public void onContainerAborted(ContainerStatus containerStatus) {
		numAllocatedContainers.decrementAndGet();
		numRequestedContainers.decrementAndGet();
	}

	public void onContainerFailed(ContainerStatus containerStatus) {
		numCompletedContainers.incrementAndGet();
		numFailedContainers.incrementAndGet();
	}

	public void onContainerCompleted(ContainerStatus containerStatus) {
		numCompletedContainers.incrementAndGet();
	}

	public abstract boolean checkDependencies();

	public abstract void stop();

	public boolean hasCompleted() {
		return completedTasks.size() == phase.getTaskList().size();
	}

	public AtomicInteger getNumRequestedContainers() {
		return numRequestedContainers;
	}

	public void setNumRequestedContainers(AtomicInteger numRequestedContainers) {
		this.numRequestedContainers = numRequestedContainers;
	}

	public AtomicInteger getNumAllocatedContainers() {
		return numAllocatedContainers;
	}

	public void setNumAllocatedContainers(AtomicInteger numAllocatedContainers) {
		this.numAllocatedContainers = numAllocatedContainers;
	}

	public AtomicInteger getNumCompletedContainers() {
		return numCompletedContainers;
	}

	public void setNumCompletedContainers(AtomicInteger numCompletedContainers) {
		this.numCompletedContainers = numCompletedContainers;
	}

	public AtomicInteger getNumFailedContainers() {
		return numFailedContainers;
	}

	public void setNumFailedContainers(AtomicInteger numFailedContainers) {
		this.numFailedContainers = numFailedContainers;
	}
	
}
