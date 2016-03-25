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

package com.tito.easyyarn.appmaster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
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
import com.tito.easyyarn.constant.YarnConstants;
import com.tito.easyyarn.phase.Phase;
import com.tito.easyyarn.phase.PhaseManager;
import com.tito.easyyarn.phase.PhaseStatus;
import com.tito.easyyarn.util.ExtendedGnuParser;

public abstract class ApplicationMaster  {

	private Options options;

	private Map<String, String> passedArguments = new HashMap<>();

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	List<Phase> phaseList = new ArrayList<>();
	Queue<Phase> pendingPhases = new LinkedList<>();
	Queue<Phase> failedPhases = new LinkedList<>();
	Queue<Phase> completedPhases = new LinkedList<>();

	List<Thread> phaseThreads = new ArrayList<>();
	Phase currentPhase;

	public void registerPhase(Phase phase) {
		phaseList.add(phase);
		pendingPhases.add(phase);
	}

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

	private volatile boolean done;

	private ByteBuffer allTokens;

	private TimeLinePublisher timeLinePublisher;
	
	public abstract void setupOptions(Options opts);
	public abstract boolean init(CommandLine cliParser) ;

	public static Options getMainClassOption() {
		Options ops = new Options();
		ops.addOption("appMasterClass", true, "Application Master Class");
		return ops;
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			Options ops = getMainClassOption();
			CommandLine cliParser = new ExtendedGnuParser(true).parse(ops, args);
			if (!cliParser.hasOption("appMasterClass")) {
				throw new RuntimeException("AppMasterClass is not specified failed to load Application Master");
			}

			ApplicationMaster appMaster = (ApplicationMaster) Class.forName(cliParser.getOptionValue("appMasterClass"))
					.newInstance();

			LOG.info("Initializing ApplicationMaster");
			appMaster.setupOptionsAll();	
			////reparse args to assign options 
			cliParser = new ExtendedGnuParser(true).parse(appMaster.getOptions(), args);
			boolean doRun = appMaster.initAll(cliParser);

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

	public Options setupOptionsAll() {
		options = getMainClassOption();
		setupDefaultOptions(options);
		setupOptions(options);
		return options;

	}

	

	private Options setupDefaultOptions(Options opts) {
		opts.addOption("jar", true, "Jar file containing the Workers");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
		return opts;
	}

	/**
	 * Parse command line options
	 *
	 * @param args
	 *            Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean initAll(CommandLine cliParser) throws ParseException, IOException {

		if (cliParser.hasOption("help")) {
			printUsage(getOptions());
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

		timeLinePublisher = new TimeLinePublisher(conf);
		if (!init(cliParser)) {
			return false;
		}

		// save passed arguments
		for (Option op : cliParser.getOptions()) {
			passedArguments.put(op.getOpt(), cliParser.getOptionValue(op.getOpt()));
		}

		return true;
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

		try {
			timeLinePublisher.publishApplicationAttemptEvent(appAttemptID.toString(), DSEvent.DS_APP_ATTEMPT_END);
		} catch (Exception e) {
			LOG.error("App Attempt start event coud not be pulished for " + appAttemptID.toString(), e);
		}
		registerPhases();

	}

	protected abstract void registerPhases();

	public boolean hasCompleted() {
		return completedPhases.size() == phaseList.size();
	}

	public boolean hasCompletedSuccessfully() {
		return hasCompleted() && failedPhases.size() == 0;
	}

	@VisibleForTesting
	protected boolean finish() {

		// wait for completion.
		LOG.info("(!done " + (!done));
		LOG.info("!hasCompleted()" + (!hasCompleted()));
		while (!done && !hasCompleted()) {
			try {
				if (currentPhase == null) {
					if (!pendingPhases.isEmpty()) {
						currentPhase = pendingPhases.poll();
						currentPhase.setPassedArguments(passedArguments);
						Thread phaseThread = new Thread(currentPhase);
						phaseThreads.add(phaseThread);
						LOG.info("Starting First Phase:" + currentPhase.getId());
						phaseThread.start();
					} else {
						LOG.error("NO Phases Registered , aborting phase execution");
						done = true;
					}
					continue;
				}
				PhaseStatus currentPhaseStatus = currentPhase.getPhaseStatus();

				if (currentPhaseStatus != null && currentPhaseStatus != PhaseStatus.RUNNING
						&& currentPhaseStatus != PhaseStatus.PENDING) {
					LOG.info("currentPhase.getPhaseStatus()" + currentPhaseStatus);
					if (currentPhaseStatus == PhaseStatus.SUCCESSED) {
						LOG.info("Phase Completed successfully : " + currentPhase.getId());
						completedPhases.add(currentPhase);
						// check to see if any pending phases start them
						if (pendingPhases.isEmpty()) {
							LOG.info("No More Phases remaining");
							done = true;
						}
						if (!pendingPhases.isEmpty()) {
							currentPhase = pendingPhases.poll();
							currentPhase.setPassedArguments(passedArguments);
							Thread phaseThread = new Thread(currentPhase);
							phaseThreads.add(phaseThread);
							phaseThread.start();
						}
					}
					// phase failed
					else {
						failedPhases.add(currentPhase);
						done = true;
					}
				}
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
			}
		}

		// Join all launched threads
		// needed for when we time out
		// and we need to release containers
		for (Thread phaseThread : phaseThreads) {
			try {
				phaseThread.join(10000);
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
		LOG.info("hasCompletedSuccessfully():" + hasCompletedSuccessfully());
		LOG.info("completedPhases.size():" + completedPhases.size());
		LOG.info("phaseList.size():" + phaseList.size());
		LOG.info("+failedPhases.size():" + failedPhases.size());
		if (hasCompletedSuccessfully()) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total Phases=" + phaseList.size() + ", completed=" + completedPhases.size()
					+ ", failed=" + failedPhases.size();
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

	public float getProgress() {
		if (phaseList == null || phaseList.isEmpty()) {
			return 0;
		}
		return completedPhases.size() / phaseList.size();
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
			LOG.error("extractTokens error={}", e);

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
			LOG.error("getAppJarResource error={}", e);
		}
		return null;

	}

	public Options getOptions() {
		return options;
	}

	public void setOptions(Options options) {
		this.options = options;
	}

	public ApplicationAttemptId getAppAttemptID() {
		return appAttemptID;
	}

	public void setAppAttemptID(ApplicationAttemptId appAttemptID) {
		this.appAttemptID = appAttemptID;
	}

	public ByteBuffer getAllTokens() {
		return allTokens;
	}

	public void setAllTokens(ByteBuffer allTokens) {
		this.allTokens = allTokens;
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
