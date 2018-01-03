package com.flipkart.cosmosdb.load.utils;


public interface Store {

	String getAnyId();

	void save(String keyStr, Object document);

	Object get(String key);


}
