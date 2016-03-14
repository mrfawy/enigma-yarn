package com.tito.enigma.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.tito.enigma.component.PlugBoard;
import com.tito.enigma.component.Reflector;
import com.tito.enigma.component.Rotor;
import com.tito.enigma.component.Util;
import com.tito.enigma.config.MachineConfig;
import com.tito.enigma.config.RotorConfig;

public class StreamGenerator {
	
	private static final int BUFFER_SIZE = 256 * 1000;
	MachineConfig machineConfig;
	List<Rotor> rotors;
	Reflector reflector;
	PlugBoard plugBoard;

	public StreamGenerator(MachineConfig machineConfig) {

		this.machineConfig = machineConfig;
		rotors = new ArrayList<>();
		for (RotorConfig rc : machineConfig.getRotorConfigs()) {
			rotors.add(new Rotor(rc));
		}
		reflector = new Reflector(machineConfig.getReflectorConfig());
		plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
	}

	public boolean generateLength(long n,OutputStream outputStream) throws IOException {
		ByteBuffer buffer=ByteBuffer.allocate(BUFFER_SIZE);
		buffer.clear();
		
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
			} while (rotateFlag == true);

			buffer.put(input);
			if (!buffer.hasRemaining()) {
				outputStream.write(buffer.array());
				buffer.clear();
			}

		}
		// copy the rest of n if exists
		if (buffer.position() != 0) {
			byte[] data=new byte[buffer.position()];
			buffer.rewind();
			buffer.get(data);
			outputStream.write(data);
			buffer.clear();
		}
		return true;
	}

}
