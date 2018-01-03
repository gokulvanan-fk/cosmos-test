package com.flipkart.cosmosdb.load.utils;

import com.microsoft.azure.documentdb.Document;

public class EmptyValidator implements Validate {

	@Override
	public boolean validateGet(String key, Document response) {
		//Dont validate
		return true;
	}

}
