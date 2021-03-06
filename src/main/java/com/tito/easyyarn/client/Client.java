package com.tito.easyyarn.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.tito.easyyarn.appmaster.ApplicationMaster;
import com.tito.easyyarn.appmaster.TokenExtractor;
import com.tito.easyyarn.constant.YarnConstants;
import com.tito.easyyarn.util.ExtendedGnuParser;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

	private static final Log LOG = LogFactory.getLog(Client.class);

	// Configuration
	private Configuration conf;
	private YarnClient yarnClient;

	private String jarPath;
	private String appMasterClass;
	// Queue for App master
	private String amQueue = "";

	// No. of containers in which the shell script needs to be executed
	private int numContainers = 1;

	// Start time for client
	private final long clientStartTime = System.currentTimeMillis();

	// flag to indicate whether to keep containers across application attempts.
	private boolean keepContainers = false;

	// Debug flag
	boolean debugFlag = false;

	// Command line options
	private Options opts;
	
	private Map<String, String> appMasterArgs=new HashMap<>();
	

	public static void main(String[] args) {
		boolean result = false;
		try {
			Client client = new Client();
			LOG.info("Initializing Client");
			try {
				boolean doRun = client.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				client.printUsage();
				System.exit(-1);
			}
			result = client.run();
		} catch (Throwable t) {
			LOG.fatal("Error running CLient", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Application completed successfully");
			System.exit(0);
		}
		LOG.error("Application failed to complete successfully");
		System.exit(2);
	}

	Client(Configuration conf) {
		this.conf = conf;

		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		opts = new Options();
		
		opts.addOption("appMasterClass", true, "Applicaiton Master class name");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("timeout", true, "Application timeout in milliseconds");
		opts.addOption("jar", true, "Jar file containing the application master");		
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");

	}

	public Client() throws Exception {
		this(new YarnConfiguration());
	}

	private void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}

	/**
	 * Parse command line options
	 */
	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new ExtendedGnuParser(true).parse(opts, args);
		
		if(!cliParser.hasOption("appMasterClass")){
			throw new IllegalArgumentException("Missing ApplicationMaster class ");
		}else{			
			try {							
				appMasterClass=cliParser.getOptionValue("appMasterClass");
				ApplicationMaster inspectedAppMaster= (ApplicationMaster) Class.forName(appMasterClass).newInstance();
				Options inspectedAppMasterOpts=inspectedAppMaster.setupOptionsAll();
				for(Object op:inspectedAppMasterOpts.getOptions()){					
					opts.addOption((Option)op);
				}
				//reparse args to assign options 
				cliParser = new ExtendedGnuParser(true).parse(opts, args);
			} catch (ClassNotFoundException e) {		
				LOG.error("Class not found :"+appMasterClass);
				throw new IllegalArgumentException("Class not found :"+appMasterClass);
			} catch (InstantiationException e) {
				LOG.error("Application Master inspection Failed:{}",e);
				throw new IllegalArgumentException("Application Master inspection Failed:"+appMasterClass);
			} catch (IllegalAccessException e) {
				LOG.error("Application Master inspection Failed:{}",e);
				throw new IllegalArgumentException("Application Master inspection Failed:"+appMasterClass);
			}
		}

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}		
		

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("debug")) {
			debugFlag = true;

		}
		
		

		if (cliParser.hasOption("keep_containers_across_application_attempts")) {
			LOG.info("keep_containers_across_application_attempts");
			keepContainers = true;
		}

		amQueue = cliParser.getOptionValue("queue", "cpsetlh");

		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("No jar file specified for application master");
		} else {
			jarPath = cliParser.getOptionValue("jar");
		}
		
		//save args

		for(Option o:cliParser.getOptions()){
			String value=cliParser.getOptionValue(o.getOpt());
			if(value!=null&&!value.isEmpty())
			appMasterArgs.put(o.getOpt(),value);			
		}
		LOG.info("Passing args app master:"+appMasterArgs.toString());
		return true;
	}

	private void printClusterReport() throws YarnException, IOException {
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOG.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		LOG.info("Got Cluster node info from ASM");
		for (NodeReport node : clusterNodeReports) {
			LOG.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress"
					+ node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers"
					+ node.getNumContainers());
		}

		QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
		LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
				+ queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
				+ ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount="
				+ queueInfo.getChildQueues().size());

		List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
			for (QueueACL userAcl : aclInfo.getUserAcls()) {
				LOG.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
						+ userAcl.name());
			}
		}
	}

	/**
	 * Main run function for the client
	 * 
	 * @return true if application completed successfully
	 * @throws IOException
	 * @throws YarnException
	 */
	public boolean run() throws IOException, YarnException {

		LOG.info("Running Client");
		yarnClient.start();
		printClusterReport();

		// Get a new application id
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

		// get resource capabilities from RM and change memory ask
		int maxMem = appResponse.getMaximumResourceCapability().getMemory();
		LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

		// A resource ask cannot exceed the max.
		if (YarnConstants.APP_MASTER_MEMORY > maxMem) {
			throw new RuntimeException("Application Master Memory is above max allowed memory on cluster");
		}

		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		LOG.info("Max virtual cores capabililty of resources in this cluster " + maxVCores);

		if (YarnConstants.APP_MASTER_VCORES > maxVCores) {
			throw new RuntimeException("Application Master VCores is above max allowed memory on cluster");
		}

		// set the application name
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();

		appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
		appContext.setApplicationName(YarnConstants.APP_NAME);

		ContainerLaunchContext amContainer = ApplicationMasterLaunchContextFactory.createAppMasterLaunchContext(conf,
				appId.toString(), jarPath,appMasterClass,appMasterArgs);

		// Set up resource type requirements
		Resource capability = Resource.newInstance(YarnConstants.APP_MASTER_MEMORY, YarnConstants.APP_MASTER_VCORES);
		appContext.setResource(capability);

		amContainer.setTokens(TokenExtractor.extractTokens(conf));

		appContext.setAMContainerSpec(amContainer);

		Priority pri = Priority.newInstance(YarnConstants.APP_MASTER_PRIORITY);
		appContext.setPriority(pri);

		// Set the queue to which this application is to be submitted in the RM
		appContext.setQueue(amQueue);

		// Submit the application to the applications manager
		LOG.info("Submitting application to ASM");
		yarnClient.submitApplication(appContext);

		// Monitor the application
		return monitorApplication(appId);

	}

	/**
	 * Monitor the submitted application for completion. Kill application if
	 * time expires.
	 * 
	 * @param appId
	 *            Application Id of application to be monitored
	 * @return true if application completed successfully
	 * @throws YarnException
	 * @throws IOException
	 */
	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {

		while (true) {

			// Check app status every 1 second.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime() + ", yarnAppState="
					+ report.getYarnApplicationState().toString() + ", distributedFinalState="
					+ report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;
				} else {
					LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
							+ ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
					return false;
				}
			} else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus="
						+ dsStatus.toString() + ". Breaking monitoring loop");
				return false;
			}

			if (System.currentTimeMillis() > (clientStartTime + YarnConstants.APP_TIMEOUT)) {
				LOG.info("Reached client specified timeout for application. Killing application");
				forceKillApplication(appId);
				return false;
			}
		}

	}

	/**
	 * Kill a submitted application by sending a call to the ASM
	 */
	private void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
		yarnClient.killApplication(appId);
	}

}
