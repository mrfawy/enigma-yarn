package com.tito.enigma.queue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Queue {
	private static final Log LOG = LogFactory.getLog(Queue.class);

	private static Queue me;
	private Map<String, byte[]> db;

	private Queue() {
		db = new HashMap<>();
	}

	public static Queue geteInstance() {
		if (me == null) {
			me = new Queue();
		}
		return me;
	}

	public void put(String id, String offset, byte[] data) {
		LOG.info("writing to queue : " + id + "_" + offset);
		System.out.println("ID:" + id + "->" + data);
		db.put(id + "_" + offset, data);

	}

	public byte[] get(String id, String index) {
		return db.get(id + "_" + index);
	}
}
