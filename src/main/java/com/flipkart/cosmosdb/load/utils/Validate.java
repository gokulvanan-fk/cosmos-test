package com.flipkart.cosmosdb.load.utils;

import com.microsoft.azure.documentdb.Document;

public interface Validate {

	boolean validateGet(String key, Document response);

}
