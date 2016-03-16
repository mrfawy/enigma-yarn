package com.tito.enigma.component;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.tito.enigma.avro.EnigmaKey;

public class EnigmaKeyUtil {
	private static final Log LOG = LogFactory.getLog(EnigmaKeyUtil.class);
	
	private static EnigmaKey enigmaKey=null;

	public static EnigmaKey loadKey(String keyPath) {
		if(enigmaKey!=null){
			return enigmaKey;
		}
		Configuration conf = new Configuration();
		SeekableInput input = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path keyFile = new Path(keyPath);
			if (!fs.exists(keyFile)) {
				LOG.error("Key not found ," + keyFile);
				return null;
			}
			LOG.info("Found Key:"+keyFile);
			input = new FsInput(keyFile, conf);

			DatumReader<EnigmaKey> keyDatumReader = new SpecificDatumReader<EnigmaKey>(EnigmaKey.class);
			DataFileReader<EnigmaKey> dataFileReader = new DataFileReader<EnigmaKey>(input, keyDatumReader);
			if(!dataFileReader.hasNext()){
				LOG.error("DataFileReader has no elements, check key file contents");
			}
			enigmaKey = dataFileReader.next();
			if(enigmaKey==null){
				LOG.error("EnigmaKey loading failed");
			}				
			dataFileReader.close();
			input.close();
			return enigmaKey;
		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return null;
		}
	}
}
