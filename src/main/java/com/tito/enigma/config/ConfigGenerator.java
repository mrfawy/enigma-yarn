package com.tito.enigma.config;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.tito.enigma.component.Util;

public class ConfigGenerator {

	public MachineConfig generateConfiguration(int minRotorCount, int maxRotorCount) {
		MachineConfig config = new MachineConfig();
		SecureRandom r = new SecureRandom();
		int rotorCount = r.nextInt(maxRotorCount) + minRotorCount;
		List<RotorConfig> rotorConfigs = new ArrayList<>();
		for (int i = 0; i < rotorCount; i++) {
			rotorConfigs.add(generateRotorConfig());
		}
		config.setRotorConfigs(rotorConfigs);
		config.setPlugBoardConfig(generatePlugBoardConfig());
		config.setReflectorConfig(generateReflectorConfig());

		return config;
	}

	@VisibleForTesting
	protected RotorConfig generateRotorConfig() {
		SecureRandom r = new SecureRandom();
		RotorConfig config = new RotorConfig();
		config.setMap(Shuffler.getShuffledArray(256));
		config.setOffset((byte) (r.nextInt(256)));
		int notchCount = r.nextInt(256) / 2;
		Set<Byte> notches = new HashSet<>();
		for (int i = 0; i < notchCount; i++) {
			notches.add((byte) r.nextInt(256));
		}
		config.setNotchSet(notches);
		return config;
	}

	@VisibleForTesting
	protected Map<Byte, Byte> generatePlugBoardConfig() {
		SecureRandom r = new SecureRandom();
		int plugBoardConnections = r.nextInt(256 / 2);
		BiMap<Byte, Byte> plugBoard = HashBiMap.create(plugBoardConnections);
		byte[] shuffledValues = Shuffler.getShuffledArray(256);
		Map<Byte, Byte> valueMap = new HashMap<Byte, Byte>();
		for (int i = 0; i < plugBoardConnections; i++) {
			valueMap.put((byte) shuffledValues[i], shuffledValues[i + 1]);			
		}
		return plugBoard;

	}

	@VisibleForTesting
	protected byte[] generateReflectorConfig() {
		byte[] map = Shuffler.getShuffledArray(256);
		byte[] result = Util.getArray(256);

		for (int i = 0; i < map.length - 1; i += 2) {
			result[Util.toUnsigned(map[i])] = map[i + 1];
			result[Util.toUnsigned(map[i + 1])] = map[i];

		}
		return result;

	}

	public static void main(String[] args) {
		ConfigGenerator cg = new ConfigGenerator();
		Map<Byte, Byte> m = cg.generatePlugBoardConfig();
		for (Byte k : m.keySet()) {
			System.out.println(k + "->" + m.get(k));
		}
	}
}
