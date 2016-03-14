package com.tito.enigma.component;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.config.EnigmaKey;

public class EnigmaKeyUtil {
	private static final Log LOG = LogFactory.getLog(EnigmaKeyUtil.class);

	public static EnigmaKey loadKey(String keyPath) {
		Configuration conf = new Configuration();
		FSDataInputStream fin = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			Path keyFile = new Path(keyPath);
			if (!fs.exists(keyFile)) {
				LOG.error("Key not found ," + keyFile);
				return null;
			}
			fin = fs.open(keyFile);
			String keyJson = fin.readUTF();
			EnigmaKey enigmaKey = new ObjectMapper().readValue(keyJson, EnigmaKey.class);
			return enigmaKey;
		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return null;
		} finally {
			if (fin != null) {
				try {
					fin.close();
				} catch (IOException e) {
					LOG.error("error={}", e);
				}
			}
		}
	}
}
