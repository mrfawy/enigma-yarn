package com.tito.sampleapp.helloworld;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.easyyarn.task.Tasklet;

public class HelloWorldTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(HelloWorldTasklet.class);

	@Override
	public boolean init(CommandLine commandLine) {
		return true;
	}

	@Override
	public void setupOptions(Options opts) {

	}

	@Override
	public boolean start() {
		try {
			/*
			 * User user1 = new User(); user1.setName("Alyssa");
			 * user1.setFavoriteNumber(256); // Leave favorite color null
			 * 
			 * // Alternate constructor User user2 = new User("Ben", 7, "red");
			 * 
			 * // Construct via builder User user3 = User.newBuilder()
			 * .setName("Charlie") .setFavoriteColor("blue")
			 * .setFavoriteNumber(null) .build();
			 * 
			 * DatumWriter<User> userDatumWriter = new
			 * SpecificDatumWriter<User>(User.class); DataFileWriter<User>
			 * dataFileWriter = new DataFileWriter<User>(userDatumWriter);
			 * 
			 * Configuration conf = new Configuration(); FileSystem fs =
			 * FileSystem.get(conf); FSDataOutputStream fout = fs.create(new
			 * Path("/axp/gcp/cpsetlh/dev/test/enigma/users.avro"));
			 * dataFileWriter.create(user1.getSchema(),fout);
			 * dataFileWriter.append(user1); dataFileWriter.append(user2);
			 * dataFileWriter.append(user3); dataFileWriter.close();
			 * fout.close();
			 */
			System.out.println("Hello world from HelloWorldTasklet!");
			return true;

		} catch (Exception ex) {
			LOG.error("Failed", ex);
			return false;
		}

	}

}
