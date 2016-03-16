package com.tito.enigma.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.enigma.avro.MachineConfig;
import com.tito.enigma.avro.RotorConfig;
import com.tito.enigma.component.PlugBoard;
import com.tito.enigma.component.Reflector;
import com.tito.enigma.component.Rotor;
import com.tito.enigma.component.Util;

public class StreamGenerator {
	private static final Log LOG = LogFactory.getLog(StreamGenerator.class);
	private static final int BUFFER_SIZE = 256 * 1000;
	MachineConfig machineConfig;
	List<Rotor> rotors;
	Reflector reflector;
	PlugBoard plugBoard;

	public StreamGenerator(MachineConfig machineConfig) {

		this.machineConfig = machineConfig;
		LOG.info("SPEC:"+machineConfig);
		LOG.info("SPEC_RC:"+machineConfig.getRotorConfigs());
		rotors = new ArrayList<>();
		for (RotorConfig rc : machineConfig.getRotorConfigs()) {
			rotors.add(new Rotor(rc));
		}
		reflector = new Reflector(machineConfig.getReflectorConfig());
		plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
	}

	public boolean generateLength(long n, OutputStream outputStream) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
		buffer.clear();
		LOG.info("Generating Length:"+n);
		for (long i = 0; i < n; i++) {
			byte[] input = Util.getArray(256);
			input = plugBoard.signalIn(input);
			for (Rotor r : rotors) {
				input = r.signalIn(input);
				r.rotate();
			}
			input = reflector.signalIn(input);
			for (int j = rotors.size() - 1; j >= 0; j--) {
				input = rotors.get(j).reverseSignalIn(input);
			}
			input = plugBoard.reverseSignalIn(input);

			// process stepping/rotating
			boolean rotateFlag;
			int rotorIndex = 0;
			do {
				rotateFlag = rotors.get(rotorIndex++).rotate();
			} while (rotateFlag == true && rotorIndex<rotors.size());

			buffer.put(input);
			if (!buffer.hasRemaining()) {
				buffer.flip();				
				outputStream.write(Util.toArray(buffer));
				buffer.clear();
			}

		}
		// copy the rest of n if exists
		if (buffer.position() != 0) {				
			buffer.flip();			
			outputStream.write(Util.toArray(buffer));
			buffer.clear();
		}
		return true;
	}

}
