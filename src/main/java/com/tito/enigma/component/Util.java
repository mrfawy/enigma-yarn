package com.tito.enigma.component;

public class Util {
	public static int toUnsigned(byte b) {
		return b & 0xFF;
	}
	
	public static byte[] getArray(int n){
		byte[] x = new byte[n];
		for (int i = 0; i < n; i++) {
			x[i] = (byte) i;
		}
		return x;
	}
}
