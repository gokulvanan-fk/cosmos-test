package com.flipkart.cosmosdb.load.data;

import org.apache.commons.lang3.RandomStringUtils;

import com.flipkart.cosmosdb.load.pojo.OneKBData;
import com.flipkart.cosmosdb.load.utils.ArchiveLogger;

public class OneKBLoad implements LoadData{

	private  static final String[] accIds = new String[1000000];
	
	public OneKBLoad(){
		for(int i=0; i<accIds.length; i++){
			accIds[i] = new StringBuilder("ACC")
			.append(System.nanoTime()).toString();
		}
	}
	
	public static String generateAccountId(String id) {
		int index= Math.abs(id.hashCode()) % 1000;
		return accIds[index];
	}
	
	public static String generateRandomName(){
		 return RandomStringUtils.randomAlphabetic(5, 10);
	}
	
	
	@Override
	public String getLabel() {
		return "1KB";
	}

	@Override
	public String getDocumentLink(String key) {
		
		return docPathPrefix+key ;
	}

	@Override
	public String getCollectionLink() {
		return collPath;
	}

	@Override
	public Object buildObject(String keyStr) {
		OneKBData data =  new OneKBData(keyStr);
		ArchiveLogger.log(data);
		return data;
	}

	
}
