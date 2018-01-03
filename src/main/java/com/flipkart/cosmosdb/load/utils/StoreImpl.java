package com.flipkart.cosmosdb.load.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Nonblocking implementation of Store
 * @author gokulvanan.v
 *
 */

public class StoreImpl implements Store {

	private final ConcurrentMap<String, Object> dataMap;
	private final String[] keys;
	private final AtomicInteger counter = new AtomicInteger(0);

	public StoreImpl(int size, int concc) {
		this.keys = new String[size];
		this.dataMap = new ConcurrentHashMap<>(size*2,concc);
	}

	@Override
	public String getAnyId() {
		int size = (counter.get() > keys.length) ? keys.length : counter.get();
		int index = (int) (System.nanoTime() % size);
		return keys[index];
	}

	@Override
	public void save(String keyStr, Object document) {
		int val = counter.getAndIncrement();
		int index = val % keys.length; // defensive;
		keys[index] = keyStr;
		dataMap.putIfAbsent(keyStr, document);
	}

	@Override
	public Object get(String key) {
		return dataMap.get(key);
	}

}
