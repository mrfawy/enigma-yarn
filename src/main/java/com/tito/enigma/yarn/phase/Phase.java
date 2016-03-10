package com.tito.enigma.yarn.phase;

public class Phase implements Runnable {

	private String id;
	private PhaseManager phaseManager;
	private PhaseStatus phaseStatus;

	@Override
	public void run() {
		phaseManager.start();
		phaseStatus=PhaseStatus.RUNNING;
		while(!phaseManager.hasCompleted()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(phaseManager.hasCompletedSuccessfully()){
			phaseStatus=PhaseStatus.SUCCESSED;
		}
		else{
			phaseStatus=PhaseStatus.FAILED;
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
	}

	public PhaseStatus getPhaseStatus() {
		return phaseStatus;
	}

	public void setPhaseStatus(PhaseStatus phaseStatus) {
		this.phaseStatus = phaseStatus;
	}

}
