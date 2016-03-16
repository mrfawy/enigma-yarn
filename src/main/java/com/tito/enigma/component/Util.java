package com.tito.enigma.component;

import java.nio.ByteBuffer;

public class Util {
	public static int toUnsigned(byte b) {
		return b & 0xFF;
	}

	public static byte[] getArray(int n) {
		byte[] x = new byte[n];
		for (int i = 0; i < n; i++) {
			x[i] = (byte) i;
		}
		return x;
	}

	public static byte[] reverseByteMap(byte[] map) {
		byte[] result = new byte[map.length];
		for (int i = 0; i < map.length; i++) {
			result[Util.toUnsigned(map[i])] = (byte) i;
		}
		return result;
	}

	public static ByteBuffer toBuffer(byte[] arr) {
		ByteBuffer buffer = ByteBuffer.allocate(arr.length);
		buffer.put(arr);
		buffer.flip();
		return buffer.duplicate();
	}

	public static byte[] toArray(ByteBuffer buffer) {		
		byte[] arr = new byte[buffer.limit()];
		buffer.get(arr);
		return arr;
	}

}
