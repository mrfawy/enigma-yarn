package com.tito.enigma.config;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.tito.enigma.avro.MachineConfig;
import com.tito.enigma.avro.RotorConfig;
import com.tito.enigma.avro.WiringPair;
import com.tito.enigma.component.Util;

public class ConfigGenerator {

	public MachineConfig generateConfiguration(int minRotorCount, int maxRotorCount) {
		MachineConfig config = new MachineConfig();
		SecureRandom r = new SecureRandom();
		int rotorCount = r.nextInt(maxRotorCount - minRotorCount + 1) + minRotorCount;
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
		config.setMap(Util.toBuffer(Shuffler.getShuffledArray(256)));
		config.setOffset(r.nextInt(256));
		int notchCount = r.nextInt(256) / 2;
		byte[] notches = new byte[notchCount];
		for (int i = 0; i < notchCount; i++) {
			notches[i] = (byte) r.nextInt(256);
		}
		config.setNotchSet(Util.toBuffer(notches));
		return config;
	}

	@VisibleForTesting
	protected List<WiringPair> generatePlugBoardConfig() {
		SecureRandom r = new SecureRandom();
		int plugBoardConnections = r.nextInt(256 / 2);
		List<WiringPair> plugBoardConfig = new ArrayList<>();
		byte[] shuffledValues = Shuffler.getShuffledArray(256);
		for (int i = 0; i < plugBoardConnections; i+=2) {
			WiringPair pair = new WiringPair();
			pair.setFrom(Util.toUnsigned(shuffledValues[i]));
			pair.setTo(Util.toUnsigned(shuffledValues[i + 1]));
			plugBoardConfig.add(pair);
		}

		return plugBoardConfig;

	}

	@VisibleForTesting
	protected ByteBuffer generateReflectorConfig() {
		byte[] map = Shuffler.getShuffledArray(256);
		byte[] result = Util.getArray(256);

		for (int i = 0; i < map.length - 1; i += 2) {
			result[Util.toUnsigned(map[i])] = map[i + 1];
			result[Util.toUnsigned(map[i + 1])] = map[i];

		}
		ByteBuffer buffer = Util.toBuffer(result);	
		return buffer;

	}

}
