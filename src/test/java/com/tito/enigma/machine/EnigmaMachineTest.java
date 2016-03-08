package com.tito.enigma.machine;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.machine.config.ConfigGenerator;

public class EnigmaMachineTest {

	@Test
	public void initTest() {
		EnigmaMachine en = new EnigmaMachine(new ConfigGenerator().generateConfiguration(10, 10));
		Assert.assertNotNull(en);
	}
	
	@Test
	public void generateLengthTest(){
		EnigmaMachine en = new EnigmaMachine(new ConfigGenerator().generateConfiguration(10, 10));
		en.generateLength(1);
	}

}
