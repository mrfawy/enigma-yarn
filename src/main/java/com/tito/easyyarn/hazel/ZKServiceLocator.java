package com.tito.easyyarn.hazel;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;

public class ZKServiceLocator {

	public static List<String> queryOtherInstancesInZk(String name, ServiceDiscovery<Void> serviceDiscovery)
			throws Exception {

		List<String> result = new ArrayList<>();
		for (ServiceInstance<Void> instance : serviceDiscovery.queryForInstances(name)) {
			result.add(instance.buildUriSpec());
		}
		return result;
	}
}
