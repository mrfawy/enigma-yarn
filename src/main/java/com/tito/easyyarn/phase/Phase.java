package com.tito.easyyarn.phase;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Phase implements Runnable {

	private static final Log LOG = LogFactory.getLog(Phase.class);

	private String id;
	private PhaseManager phaseManager;
	private PhaseStatus phaseStatus;
	private Map<String,String> passedArguments;

	

	@Override
	public void run() {
		setPhaseStatus(PhaseStatus.RUNNING);
		phaseManager.start();
		while (!phaseManager.hasCompleted()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (phaseManager.hasCompletedSuccessfully()) {
			LOG.info("Phase Manager Completed Successfully");
			setPhaseStatus(PhaseStatus.SUCCESSED);
		} else {
			LOG.info("Phase Manager Failed");
			setPhaseStatus(PhaseStatus.FAILED);
		}

	}

	public Phase(String id, PhaseManager phaseManager) {
		super();
		this.id = id;
		this.phaseManager = phaseManager;
		this.phaseManager.setPhase(this);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public PhaseManager getPhaseManager() {
		return phaseManager;
	}

	public void setPhaseManager(PhaseManager phaseManager) {
		this.phaseManager = phaseManager;
		phaseManager.setPhase(this);
	}

	public synchronized PhaseStatus getPhaseStatus() {
		return phaseStatus;
	}

	public synchronized void setPhaseStatus(PhaseStatus phaseStatus) {
		this.phaseStatus = phaseStatus;
	}

	public Map<String, String> getPassedArguments() {
		return passedArguments;
	}

	public void setPassedArguments(Map<String, String> passedArguments) {
		this.passedArguments = passedArguments;
	}

	
}
