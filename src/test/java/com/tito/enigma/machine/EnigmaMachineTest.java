package com.tito.enigma.machine;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.machine.config.ConfigGenerator;
import com.tito.enigma.machine.config.MachineConfig;
import com.tito.enigma.queue.Queue;

public class EnigmaMachineTest {

	@Test
	public void initTest() {
		EnigmaMachine en = new EnigmaMachine(new ConfigGenerator().generateConfiguration(10, 10),"1");
		Assert.assertNotNull(en);
	}
	
	@Test
	public void generateLengthTest(){
		MachineConfig conf = new ConfigGenerator().generateConfiguration(10, 10);
		EnigmaMachine en1 = new EnigmaMachine(conf,"1");
		en1.generateLength(5000);

	
		EnigmaMachine en2 = new EnigmaMachine(conf,"2");
		en2.generateLength(10);
		
		Assert.assertArrayEquals(Arrays.copyOf( Queue.geteInstance().get("1","0"),256),Arrays.copyOf(Queue.geteInstance().get("2","0"),256));
		
	}

}
