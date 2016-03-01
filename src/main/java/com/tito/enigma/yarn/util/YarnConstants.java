package com.tito.enigma.yarn.util;

import com.tito.enigma.yarn.applicationmaster.ApplicationMaster;

public class YarnConstants {
	
	public static final String APP_JAR="enigma-yarn.jar";
	public static final String APP_NAME="engima-yarn";
	public static final String APP_MASTER_CLASS=ApplicationMaster.class.getCanonicalName();
	public static final int APP_MASTER_MEMORY=256; // in MB allocated to app master
	public static final int APP_MASTER_VCORES=1;// CPU unit
	public static final int APP_MASTER_PRIORITY=0;// lowest
	
	public static final int APP_CONTAINER_MEMORY=256;// MB
	public static final int APP_CONTAINER_VCORES=1;// CPU unit
	
	
	public static final int  APP_TIMEOUT=600000;
	

}
