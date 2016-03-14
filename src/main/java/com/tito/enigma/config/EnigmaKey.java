package com.tito.enigma.config;

import java.util.List;
import java.util.Map;

public class EnigmaKey {

	private List<String> machineOrder;
	private Map<String,MachineConfig> machineConfig;
	
	
	public List<String> getMachineOrder() {
		return machineOrder;
	}
	public void setMachineOrder(List<String> machineOrder) {
		this.machineOrder = machineOrder;
	}
	public Map<String, MachineConfig> getMachineConfig() {
		return machineConfig;
	}
	public void setMachineConfig(Map<String, MachineConfig> machineConfig) {
		this.machineConfig = machineConfig;
	}
	
	
}
