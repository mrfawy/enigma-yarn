package com.tito.enigma.yarn.util;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LocalResourcesUtil {

	public static void  addToLocalResources(Configuration conf, String fileSrcPath, String fileDstPath, String appId,
			Map<String, LocalResource> localResources, String resources) throws IOException {

		FileSystem fs = FileSystem.get(conf);
		String suffix = YarnConstants.APP_NAME + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream ostream = null;
			try {
				ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
				ostream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(ostream);
			}
		} else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
				LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
				scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}
}
