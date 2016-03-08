package com.tito.enigma.machine.config;

import java.security.SecureRandom;

public class Shuffler {

	// Implementing Fisher–Yates shuffle
	static void shuffleArray(byte[] ar) {
		SecureRandom r = new SecureRandom();
		for (int i = ar.length - 1; i > 0; i--) {
			int index = r.nextInt(i + 1);
			// Simple swap
			byte a = ar[index];
			ar[index] = ar[i];
			ar[i] = a;
		}
	}

	static byte[] getShuffledArray(int length) {
		byte[] x = new byte[length];
		for (int i = 0; i < length; i++) {
			x[i] = (byte) i;
		}
		shuffleArray(x);
		return x;
	}

}
