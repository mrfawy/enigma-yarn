package com.tito.easyyarn.hazel;

import com.hazelcast.core.HazelcastInstance;

public class GridServiceAgent {

	private String id;
	private HazelcastInstance hazelInstance;
	private static GridServiceAgent me;

	public GridServiceAgent(String id) {
		super();
		this.id = id;
		init();
	}

	private void init() {
		hazelInstance = new HazelZKConfig().getInstance();
	}

	public static void initInstance(String id) {
		if (me == null) {
			me = new GridServiceAgent(id);
		} else {
			throw new RuntimeException("GridServiceAgent Instance already initialized , call getInstance instead");
		}

	}

	public static GridServiceAgent getInstance() {
		if (me == null) {
			throw new RuntimeException("Instance not initialized , call initInstance first");
		}
		return me;
	}

	public HazelcastInstance getHazelInstance() {
		return hazelInstance;
	}
	

}
