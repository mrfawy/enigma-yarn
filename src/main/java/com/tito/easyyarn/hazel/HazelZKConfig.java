package com.tito.easyyarn.hazel;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * based on : https://dzone.com/articles/hazelcast-member-discovery
 * 
 * @author mabdelra
 *
 */

public class HazelZKConfig {
	private static final Log LOG = LogFactory.getLog(HazelZKConfig.class);

	private String appID = "EASY_YARN_HAZEL";// should be yarn applicationid
	private String zooKeeperUrl = "lppbd0020.gso.aexp.com:5181";

	private int freePort = -1;

	public HazelcastInstance getInstance() {
		try {
			return Hazelcast.newHazelcastInstance(getConfig());
		} catch (Exception e) {
			LOG.error("Failed to create HazelInstance");
			return null;
		}
	}

	private Config getConfig() throws Exception {
		final Config config = new Config();
		config.setNetworkConfig(getNetworkConfig());
		config.getGroupConfig().setName(appID);
		return config;
	}

	private NetworkConfig getNetworkConfig() throws Exception {
		final NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(getJoinConfig());
		networkConfig.setPort(getFreePort());
		return networkConfig;
	}

	private JoinConfig getJoinConfig() throws Exception {
		final JoinConfig joinConfig = getDisabledMulticast();
		joinConfig.setTcpIpConfig(getTcpIpConfig());
		return joinConfig;
	}

	private JoinConfig getDisabledMulticast() {
		JoinConfig join = new JoinConfig();
		final MulticastConfig multicastConfig = new MulticastConfig();
		multicastConfig.setEnabled(false);
		join.setMulticastConfig(multicastConfig);
		return join;
	}

	private TcpIpConfig getTcpIpConfig() throws Exception {
		final TcpIpConfig tcpIpConfig = new TcpIpConfig();
		final List<String> instances = queryOtherInstancesInZk(appID, getServiceDiscovery());
		tcpIpConfig.setMembers(instances);
		tcpIpConfig.setEnabled(true);
		return tcpIpConfig;
	}

	private ServiceDiscovery<Void> getServiceDiscovery() throws Exception {

		ServiceDiscovery<Void> serviceDiscovery = ServiceDiscoveryBuilder.builder(Void.class).basePath("hazelcast")
				.client(getCuratorFramework()).thisInstance(getServiceInstance()).build();
		serviceDiscovery.start();
		return serviceDiscovery;

	}

	private ServiceInstance<Void> getServiceInstance() throws Exception {
		final String hostName = InetAddress.getLocalHost().getHostName();
		return ServiceInstance.<Void> builder().name(appID).uriSpec(new UriSpec("{address}:{port}")).address(hostName)
				.port(getFreePort()).build();
	}

	private CuratorFramework getCuratorFramework() {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFrameowrk = CuratorFrameworkFactory.newClient(zooKeeperUrl, retryPolicy);
		curatorFrameowrk.start();
		return curatorFrameowrk;
	}

	private List<String> queryOtherInstancesInZk(String name, ServiceDiscovery<Void> serviceDiscovery)
			throws Exception {

		List<String> result = new ArrayList<>();
		for (ServiceInstance<Void> instance : serviceDiscovery.queryForInstances(name)) {
			result.add(instance.buildUriSpec());
		}
		return result;
	}

	public int getFreePort() {
		if (freePort == -1) {
			try {
				ServerSocket socket = new ServerSocket(0);
				freePort = socket.getLocalPort();				
				socket.close();
			} catch (Exception e) {
				LOG.error("Can't find open port");
				return -1;
			}

		}
		return freePort;
	}

}
