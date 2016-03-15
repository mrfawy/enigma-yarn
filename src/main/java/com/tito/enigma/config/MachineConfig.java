package com.tito.enigma.config;

import java.util.List;
import java.util.Map;

public class MachineConfig {
	
	List<RotorConfig> rotorConfigs;
	byte[] reflectorConfig;
	Map<Byte, Byte> plugBoardConfig;
	
	//avro 
	List<WiringPair> plugBaordWiring;

	public List<RotorConfig> getRotorConfigs() {
		return rotorConfigs;
	}

	public void setRotorConfigs(List<RotorConfig> rotorConfigs) {
		this.rotorConfigs = rotorConfigs;
	}

	public byte[] getReflectorConfig() {
		return reflectorConfig;
	}

	public void setReflectorConfig(byte[] reflectorConfig) {
		this.reflectorConfig = reflectorConfig;
	}

	public Map<Byte, Byte> getPlugBoardConfig() {
		return plugBoardConfig;
	}

	public void setPlugBoardConfig(Map<Byte, Byte> plugBoardConfig) {
		this.plugBoardConfig = plugBoardConfig;
	}

}
