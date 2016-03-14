package com.tito.enigma.config;

import java.io.Serializable;
import java.util.Set;

public class RotorConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	byte[] map;
	byte offset;
	Set<Byte> notchSet;

	public byte[] getMap() {
		return map;
	}

	public void setMap(byte[] map) {
		this.map = map;
	}

	public byte getOffset() {
		return offset;
	}

	public void setOffset(byte offset) {
		this.offset = offset;
	}

	public Set<Byte> getNotchSet() {
		return notchSet;
	}

	public void setNotchSet(Set<Byte> notchSet) {
		this.notchSet = notchSet;
	}

}
