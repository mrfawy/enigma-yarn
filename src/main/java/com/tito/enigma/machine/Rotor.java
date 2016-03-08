package com.tito.enigma.machine;

import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.tito.enigma.machine.config.RotorConfig;

public class Rotor implements SwitchIF {

	private int offset;
	private Set<Byte> notchIndexSet;
	private BiMap<Byte, Byte> map;

	public Rotor(RotorConfig rc) {
		offset = Util.unsignedToBytes(rc.getOffset());
		notchIndexSet = rc.getNotchSet();
		map = HashBiMap.create(rc.getMap().length);
		for (int i = 0; i < rc.getMap().length; i++) {
			map.put((byte) i, rc.getMap()[i]);
		}
	}

	public byte[] signalIn(byte[] in) {
		byte[] result = new byte[256];
		for (int i = 0; i < in.length; i++) {
			int j =(offset + i) % 256;
			result[i] = map.get(in[j]);
		}
		return result;
	}

	@Override
	public byte[] reverseSignalIn(byte[] in) {
		byte[] result = new byte[256];
		for (int i = 0; i < in.length; i++) {
			int j = (offset + i) % 256;
			result[i] = map.inverse().get(in[j]);
		}
		return result;
	}

	public boolean rotate() {
		offset = (offset + 1) % 256;
		return notchIndexSet.contains(offset);

	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Set<Byte> getNotchIndexSet() {
		return notchIndexSet;
	}

	public void setNotchIndexSet(Set<Byte> notchIndexSet) {
		this.notchIndexSet = notchIndexSet;
	}

	public BiMap<Byte, Byte> getMap() {
		return map;
	}

	public void setMap(BiMap<Byte, Byte> map) {
		this.map = map;
	}

	

}
