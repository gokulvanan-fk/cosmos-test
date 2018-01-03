package com.flipkart.cosmosdb.load.data;


public interface LoadData {

	public String dbName = "transact";
	public String dbPath = "/dbs/"+dbName;
	public String collName = "orders";
	public String collPath = dbPath+"/colls/"+collName;
	public String docPathPrefix = dbPath+"/colls/"+collName+"/docs/";
    String getLabel();

    public static LoadData getLoader(String clazzName) throws Exception {
        Class<?> clazz = Class.forName(clazzName);
        return (LoadData) clazz.newInstance();
    }

	String getDocumentLink(String key);

	String getCollectionLink();

	Object buildObject(String keyStr);



}
