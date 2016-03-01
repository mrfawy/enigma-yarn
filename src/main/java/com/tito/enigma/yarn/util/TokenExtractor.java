package com.tito.enigma.yarn.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TokenExtractor {

	private static final Log LOG = LogFactory.getLog(TokenExtractor.class);

	public static ByteBuffer extractTokens(Configuration conf) {
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			if (UserGroupInformation.isSecurityEnabled()) {				
				Credentials credentials = new Credentials();
				String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
				if (tokenRenewer == null || tokenRenewer.length() == 0) {
					throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
				}

				// For now, only getting tokens for the default file-system.
				final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
				if (tokens != null) {
					for (Token<?> token : tokens) {
						LOG.info("Got dt for " + fs.getUri() + "; " + token);
					}
				}
				DataOutputBuffer dob = new DataOutputBuffer();
				credentials.writeTokenStorageToStream(dob);
				ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
				return fsTokens;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

}
