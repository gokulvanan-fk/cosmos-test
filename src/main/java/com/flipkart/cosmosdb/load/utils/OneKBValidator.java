package com.flipkart.cosmosdb.load.utils;

import com.flipkart.cosmosdb.load.pojo.OneKBData;
import com.microsoft.azure.documentdb.Document;

public class OneKBValidator implements Validate{

	private final Store store;
	
	public OneKBValidator(Store store) {
		this.store = store;
	}
	
	@Override
	public boolean validateGet(String key, Document response) {
		OneKBData expected = (OneKBData) store.get(key);
		OneKBData actual = new OneKBData(response);
		return expected.equals(actual);
	}

}
