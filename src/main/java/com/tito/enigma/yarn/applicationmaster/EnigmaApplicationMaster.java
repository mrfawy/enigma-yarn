package com.tito.enigma.yarn.applicationmaster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.enigma.yarn.phase.DummyPhase;
import com.tito.enigma.yarn.phase.FixedTasksPhaseManager;
import com.tito.enigma.yarn.phase.Phase;

public class EnigmaApplicationMaster extends ApplicationMaster {

	public void definePhases() {
		Phase phase1 = new DummyPhase("dummpy Phase");
		phase1.setPhaseManager(new FixedTasksPhaseManager(this, phase1));

	}

	public Options getOptions() {
		Options opts = new Options();
		opts.addOption("keyDir", true, "Dir to generate keys");
		return opts;
	}

	@Override
	protected boolean init(CommandLine commandLine) {

		return true;
	}

	@Override
	protected void start() {

	}
}
