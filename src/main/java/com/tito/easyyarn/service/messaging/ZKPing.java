package com.tito.easyyarn.service.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

public class ZKPing extends FILE_PING {
	public static final short ZK_PING_ID = 7001;
	public static final short ZK_PING_ID2 = 7002;

	private static final String ROOT_PATH = "/yarnina/messaging/jgroups/";

	private volatile String discoveryPath;
	private volatile String localNodePath;

	private CuratorFramework curator;

	@Property
	private String zooKeeperUrl;

	static {
		ClassConfigurator.addProtocol(ZK_PING_ID, ZKPing.class);
	}

	@Override
	public void init() throws Exception {
		super.init();
		curator = createCurator();
		curator.start();
	}

	private CuratorFramework createCurator() {
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework curatorFrameowrk = CuratorFrameworkFactory.newClient(zooKeeperUrl, retryPolicy);
		return curatorFrameowrk;
	}

	@Override
	protected void createRootDir() {
		// ignore
	}

	protected void _createRootDir() {
		try {
			if (curator.checkExists().forPath(localNodePath) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(localNodePath);
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("Failed to create dir %s in ZooKeeper.", localNodePath), e);
		}
	}

	public Object down(Event evt) {
		switch (evt.getType()) {
		case Event.CONNECT:
		case Event.CONNECT_USE_FLUSH:
		case Event.CONNECT_WITH_STATE_TRANSFER:
		case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
			discoveryPath = ROOT_PATH + evt.getArg();
			localNodePath = discoveryPath + "/" + addressAsString(local_addr);
			_createRootDir();
			break;
		}
		return super.down(evt);
	}

	@Override
	protected void readAll(final List<Address> members, final String clusterName, final Responses responses) {
		List<PingData> pingDataList = readAll(clusterName);
		for (PingData pingData : pingDataList) {
			if (members == null || members.contains(pingData.getAddress())) {
				responses.addResponse(pingData, pingData.isCoord());
				if (log.isTraceEnabled()) {
					log.trace("added member %s [members: %s]", pingData, members != null);
				}
			}

		}
	}

	protected synchronized List<PingData> readAll(String clusterName) {
		List<PingData> retval = new ArrayList<>();
		try {
			for (String node : curator.getChildren().forPath(discoveryPath)) {
				String nodePath = ZKPaths.makePath(discoveryPath, node);
				PingData nodeData = readPingData(nodePath);
				if (nodeData != null) {
					retval.add(nodeData);
				}
			}

		} catch (Exception e) {
			log.debug(String.format("Failed to read ping data from ZooKeeper for cluster: %s", clusterName), e);
		}
		return retval;
	}

	@Override
	protected synchronized void write(List<PingData> data, String clustername) {
		for (PingData pingRec : data) {
			writePingData(pingRec);
		}

	}

	protected synchronized PingData readPingData(String path) {
		PingData retval = null;
		DataInputStream in = null;
		try {
			byte[] bytes = curator.getData().forPath(path);
			in = new DataInputStream(new ByteArrayInputStream(bytes));
			PingData tmp = new PingData();
			tmp.readFrom(in);
			return tmp;
		} catch (Exception e) {
			log.debug(String.format("Failed to read ZooKeeper znode: %s", path), e);
		} finally {
			Util.close(in);
		}
		return retval;
	}

	protected synchronized void writePingData(PingData data) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = null;
		try {
			dos = new DataOutputStream(baos);

			data.writeTo(dos);

			if (curator.checkExists().forPath(localNodePath) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(localNodePath,
						baos.toByteArray());
			} else {
				curator.setData().forPath(localNodePath, baos.toByteArray());
			}
		} catch (Exception ex) {
			log.error("Error saving ping data", ex);
		} finally {
			Util.close(dos);
			Util.close(baos);
		}
	}

	protected void removeNode(String path) {
		try {
			curator.delete().forPath(path);
		} catch (KeeperException.NoNodeException e) {
			// just log this, to keep down to 1 call, instead of 2 (exists +
			// delete)
			// coord nodes already remove this in handleView(), hence
			// "expecting" this exception
			if (log.isTraceEnabled()) {
				log.trace(String.format("Node '%s' already removed: %s", path, e));
			}
		} catch (Exception e) {
			log.error(String.format("Failed removing %s", path), e);
		}
	}

	protected void remove(String clustername, Address addr) {
		removeNode(discoveryPath + "/" + addressAsString(addr));
	}

	@Override
	public void destroy() {
		curator.close();
		super.destroy();

	}

}
