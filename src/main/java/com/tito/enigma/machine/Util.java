package com.tito.enigma.machine;

public class Util {
	public static int unsignedToBytes(byte b) {
		return b & 0xFF;
	}
	
	public static byte[] getArray(int n){
		byte[] x = new byte[n];
		for (int i = 0; i < n; i++) {
			x[i] = (byte) -1;
		}
		return x;
	}
}
