package com.flipkart.cosmosdb.load;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.flipkart.cosmosdb.load.data.LoadData;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

public enum LoaderSetup {
	INSTANCE;

	String apiEndPoint ="";
	String key = "";

	private ExecutorService reqExec;
	private  DocumentClient client;

	public void setup(int connPoolSize, ConsistencyLevel level,
			int concurrency,
			LoadData loadData) throws Exception {
		this.client = createConnection(connPoolSize, level);
		this.reqExec = Executors.newFixedThreadPool(concurrency,
				new ThreadFactory() {
			private volatile int count = 0;

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "executor-" + (++count));
			}
		});

		System.out.println("Created thread pool size "+concurrency);

		System.out.println("Created connection");

	}

	private DocumentClient createConnection(int poolSize , ConsistencyLevel level){
		ConnectionPolicy policy = new ConnectionPolicy();
		policy.setConnectionMode(ConnectionMode.Gateway);
		policy.setMaxPoolSize(poolSize);
		policy.setRequestTimeout(200);
		return new DocumentClient(apiEndPoint, 
				key, 
				policy, 
				level);
	}

	public void tearDown() throws Exception {
		System.out.println("Shutting down test");
		reqExec.shutdownNow();
		client.close();
	}

	public ExecutorService getReqExec() {
		return this.reqExec;
	}

	public DocumentClient getConn() {
		return this.client;
	}

	public static void main(String[] args){
		String test ="'{\"header\":{\"appName\":\"chore\",\"configName\":\"CREATE_RETURN\",\"profile\":\"platform\",\"instanceId\":\"127.0.0.1\",\"eventId\":\"testEvent-chore1\",\"timestamp\":1500900694561,\"testRequest\":false},\"data\":{\"requestId\":\"687364823\",\"clientTraceId\":\"dsahkja\",\"client\":\"efew\",\"clientAppIPAddress\":\"dfewew\",\"clientHostName\":\"764782332\",\"choreId\":\"2kfjwhjkf\",\"restBusTxnId\":\"fewkfhwekj\",\"restBusMsgId\":\"423984923\",\"restBusUniqId\":\"dfhksjdhksd\",\"eventType\":\"CREATE_RETURN\",\"eventId\":\"wejrowejriwejijewew\",\"debugMsg\":\"oewoufweohfewhfw\",\"metricType\":\"CUSTOMER_RETURN\",\"timestamp\":1500900694559,\"subEventType\":\"WAIT_REFUND_NOTAPPROVED\"}}";
		System.out.println(test.getBytes().length);
	}
}
