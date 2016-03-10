/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tito.enigma.yarn.applicationmaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;
import com.tito.enigma.yarn.phase.Phase;
import com.tito.enigma.yarn.phase.PhaseManager;
import com.tito.enigma.yarn.util.YarnConstants;

public abstract class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	List<Phase> phaseList = new ArrayList<>();
	Phase currentPhase;

	Phase getCurrentPhase() {
		return currentPhase;
	}

	PhaseManager getCurrentPhaseManager() {
		return currentPhase.getPhaseManager();
	}

	@VisibleForTesting
	@Private
	public static enum DSEvent {
		DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
	}

	@VisibleForTesting
	@Private
	public static enum DSEntity {
		DS_APP_ATTEMPT, DS_CONTAINER
	}

	private Configuration conf;

	private String jarPath;
	private long appJarTimestamp;
	private long appJarPathLen;

	// Handle to communicate with the Resource Manager
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;

	// Handle to communicate with the Node Manager
	private NMClientAsync nmClientAsync;
	// Listen to process the response from the Node Manager
	private NMCallbackHandler containerListener;

	// Application Attempt Id ( combination of attemptId and fail count )
	protected ApplicationAttemptId appAttemptID;

	// Hostname of the container
	private String appMasterHostname = "";
	// Port on which the app master listens for status updates from clients
	private int appMasterRpcPort = -1;
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl = "";

	protected int numTotalContainers = 1;
	// Memory to request for the container on which the shell command will run
	private int containerMemory = 100;
	private int containerVirtualCores = 1;
	private int requestPriority;

	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();

	private volatile boolean done;

	private ByteBuffer allTokens;

	// Launch threads
	private List<Thread> launchThreads = new ArrayList<Thread>();

	private TimeLinePublisher timeLinePublisher;

	public PhaseManager getPhaseManager() {
		return null;
	}

	public abstract Options getOptions();

	public static ApplicationMaster getInstance() {
		return null;
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			ApplicationMaster appMaster = getInstance();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			appMaster.run();
			result = appMaster.finish();
		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			LogManager.shutdown();
			ExitUtil.terminate(1, t);
		}
		if (result) {
			LOG.info("Application Master completed successfully. exiting");
			System.exit(0);
		} else {
			LOG.info("Application Master failed. exiting");
			System.exit(2);
		}
	}

	/**
	 * Dump out contents of $CWD and the environment to stdout for debugging
	 */
	private void dumpOutDebugInfo() {

		LOG.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
		}

		BufferedReader buf = null;
		try {
			String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") : Shell.execCommand("ls", "-al");
			buf = new BufferedReader(new StringReader(lines));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOG.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.cleanup(LOG, buf);
		}
	}

	public ApplicationMaster() {
		// Set up the configuration
		conf = new YarnConfiguration();
	}

	protected abstract boolean init(CommandLine commandLine);

	/**
	 * Parse command line options
	 *
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException, IOException {
		Options opts = getOptions();
		opts.addOption("jar", true, "Jar file containing the Workers");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException("No args specified for application master to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}
		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("Missing Jar file for workers");
		}
		this.jarPath = cliParser.getOptionValue("jar");
		Map<String, String> envs = System.getenv();

		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException("Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
		}

		if (envs.containsKey(YarnConstants.APP_JAR)) {
			jarPath = envs.get(YarnConstants.APP_JAR);

			if (envs.containsKey(YarnConstants.APP_JAR_TIMESTAMP)) {
				appJarTimestamp = Long.valueOf(envs.get(YarnConstants.APP_JAR_TIMESTAMP));
			}
			if (envs.containsKey(YarnConstants.APP_JAR_LENGTH)) {
				appJarPathLen = Long.valueOf(envs.get(YarnConstants.APP_JAR_LENGTH));
			}

			if (!jarPath.isEmpty() && (appJarTimestamp <= 0 || appJarPathLen <= 0)) {
				LOG.error("Illegal values in env for jar path" + ", path=" + jarPath + ", len=" + appJarPathLen
						+ ", timestamp=" + appJarTimestamp);
				throw new IllegalArgumentException("Illegal values in env for jar  path");
			}
		}

		LOG.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
				+ ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
				+ appAttemptID.getAttemptId());

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "100"));
		containerVirtualCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException("Cannot run Engima containers with zero containers");
		}
		requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

		timeLinePublisher = new TimeLinePublisher(conf);
		return init(cliParser);
	}

	/**
	 * Helper function to print usage
	 *
	 * @param opts
	 *            Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	/**
	 * Main run function for the application master
	 *
	 * @throws YarnException
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked" })
	public void run() throws YarnException, IOException {
		LOG.info("Starting ApplicationMaster");
		try {
			timeLinePublisher.publishApplicationAttemptEvent(appAttemptID.toString(), DSEvent.DS_APP_ATTEMPT_START);
		} catch (Exception e) {
			LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
		}

		extractTokens();

		// AM to RM client listener
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler(this));
		amRMClient.init(conf);
		amRMClient.start();

		// AM to NM listener
		containerListener = new NMCallbackHandler(this);
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		// Register with ResourceManager to start heartbeating to the RM
		appMasterHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname,
				appMasterRpcPort, appMasterTrackingUrl);

		// cluster information capability as per resource manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

		int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max vcores capabililty of resources in this cluster " + maxVCores);

		// A resource ask cannot exceed the max.
		if (containerMemory > maxMem) {
			LOG.info("Container memory specified above max threshold of cluster." + " Using max value." + ", specified="
					+ containerMemory + ", max=" + maxMem);
			containerMemory = maxMem;
		}

		if (containerVirtualCores > maxVCores) {
			LOG.info("Container virtual cores specified above max threshold of cluster." + " Using max value."
					+ ", specified=" + containerVirtualCores + ", max=" + maxVCores);
			containerVirtualCores = maxVCores;
		}

		List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
		LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
				+ " previous attempts' running containers on AM registration.");
		numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

		int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size();

		// Setup ask for containers from RM
		// Send request for containers to RM
		// Until we get our fully allocated quota, we keep on polling RM for
		// containers
		// Keep looping until all the containers are launched and shell script
		// executed on them ( regardless of success/failure).
		for (int i = 0; i < numTotalContainersToRequest; ++i) {
			ContainerRequest containerAsk = setupContainerAskForRM();
			amRMClient.addContainerRequest(containerAsk);
		}
		numRequestedContainers.set(numTotalContainers);
		try {
			timeLinePublisher.publishApplicationAttemptEvent(appAttemptID.toString(), DSEvent.DS_APP_ATTEMPT_END);
		} catch (Exception e) {
			LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
		}
		start();
	}

	protected abstract void start();

	@VisibleForTesting
	protected boolean finish() {
		// wait for completion.
		while (!done && (numCompletedContainers.get() != numTotalContainers)) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {
			}
		}

		// Join all launched threads
		// needed for when we time out
		// and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		LOG.info("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		LOG.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		boolean success = true;
		if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
					+ numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed="
					+ numFailedContainers.get();
			success = false;
		}
		try {
			amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
		} catch (YarnException ex) {
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}

		amRMClient.stop();

		return success;
	}

	private void extractTokens() {
		// Credentials, Token, UserGroupInformation, DataOutputBuffer
		Credentials credentials;
		try {
			credentials = UserGroupInformation.getCurrentUser().getCredentials();
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.writeTokenStorageToStream(dob);
			// Now remove the AM->RM token so that containers cannot access it.
			Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
			LOG.info("Executing with tokens:");
			while (iter.hasNext()) {
				Token<?> token = iter.next();
				LOG.info(token);
				if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
					iter.remove();
				}
			}
			allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public LocalResource getAppJarResource() {
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		try {
			if (!jarPath.isEmpty()) {
				appMasterJar.setType(LocalResourceType.FILE);
				Path jar = new Path(jarPath);
				jar = FileSystem.get(conf).makeQualified(jar);
				appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jar));
				appMasterJar.setTimestamp(appJarTimestamp);
				appMasterJar.setSize(appJarPathLen);
				appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
			}
			return appMasterJar;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 *
	 * @return the setup ResourceRequest to be sent to RM
	 */
	public ContainerRequest setupContainerAskForRM() {

		Priority pri = Priority.newInstance(requestPriority);
		// Set up resource type requirements
		Resource capability = Resource.newInstance(containerMemory, containerVirtualCores);
		ContainerRequest request = new ContainerRequest(capability, null, null, pri);
		LOG.info("Requested container ask: " + request.toString());
		return request;
	}

	public ApplicationAttemptId getAppAttemptID() {
		return appAttemptID;
	}

	public void setAppAttemptID(ApplicationAttemptId appAttemptID) {
		this.appAttemptID = appAttemptID;
	}

	public int getNumTotalContainers() {
		return numTotalContainers;
	}

	public void setNumTotalContainers(int numTotalContainers) {
		this.numTotalContainers = numTotalContainers;
	}

	public AtomicInteger getNumCompletedContainers() {
		return numCompletedContainers;
	}

	public void setNumCompletedContainers(AtomicInteger numCompletedContainers) {
		this.numCompletedContainers = numCompletedContainers;
	}

	public AtomicInteger getNumAllocatedContainers() {
		return numAllocatedContainers;
	}

	public void setNumAllocatedContainers(AtomicInteger numAllocatedContainers) {
		this.numAllocatedContainers = numAllocatedContainers;
	}

	public ByteBuffer getAllTokens() {
		return allTokens;
	}

	public void setAllTokens(ByteBuffer allTokens) {
		this.allTokens = allTokens;
	}

	public AtomicInteger getNumFailedContainers() {
		return numFailedContainers;
	}

	public void setNumFailedContainers(AtomicInteger numFailedContainers) {
		this.numFailedContainers = numFailedContainers;
	}

	public AtomicInteger getNumRequestedContainers() {
		return numRequestedContainers;
	}

	public void setNumRequestedContainers(AtomicInteger numRequestedContainers) {
		this.numRequestedContainers = numRequestedContainers;
	}

	public TimeLinePublisher getTimeLinePublisher() {
		return timeLinePublisher;
	}

	public void setTimeLinePublisher(TimeLinePublisher timeLinePublisher) {
		this.timeLinePublisher = timeLinePublisher;
	}

	public NMCallbackHandler getContainerListener() {
		return containerListener;
	}

	public void setContainerListener(NMCallbackHandler containerListener) {
		this.containerListener = containerListener;
	}

	public NMClientAsync getNmClientAsync() {
		return nmClientAsync;
	}

	public void setNmClientAsync(NMClientAsync nmClientAsync) {
		this.nmClientAsync = nmClientAsync;
	}

	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}

	public List<Thread> getLaunchThreads() {
		return launchThreads;
	}

	public void setLaunchThreads(List<Thread> launchThreads) {
		this.launchThreads = launchThreads;
	}

	public AMRMClientAsync getAmRMClient() {
		return amRMClient;
	}

	public void setAmRMClient(AMRMClientAsync amRMClient) {
		this.amRMClient = amRMClient;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public String getJarPath() {
		return jarPath;
	}

	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}

}
