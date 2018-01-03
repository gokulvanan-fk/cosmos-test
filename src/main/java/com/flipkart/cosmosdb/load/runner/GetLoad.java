package com.flipkart.cosmosdb.load.runner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

import com.flipkart.cosmosdb.load.data.LoadData;
import com.flipkart.cosmosdb.load.profiler.ProfileMe;
import com.flipkart.cosmosdb.load.utils.Store;
import com.flipkart.cosmosdb.load.utils.Validate;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;

public class GetLoad implements LoadRunner {
	private final ExecutorService reqExec;
	private final long reqSize;
	private final int concurrency;
	private final DocumentClient client;
	private final LongAdder counter = new LongAdder();
	private final LongAdder failedCounter = new LongAdder();
	private final Store store;
	private final int missPerc;
	private final LoadData loadData;
	private final Validate validate;

	public GetLoad(ExecutorService exec, DocumentClient client, 
			Store store, Validate validate,
			long reqSize, int conc, int missPerc, LoadData loadData) {
		this.store = store;
		this.reqExec = exec;
		this.reqSize = reqSize;
		this.validate = validate;
		this.concurrency = conc;
		this.client = client;
		this.missPerc = missPerc;
		this.loadData = loadData;
		this.counter.add(reqSize*concurrency);
	}

	@Override
	public void runLoad() throws InterruptedException {
		System.out.println("running Get load req " + reqSize + " concurrency "
				+ concurrency);
		for (int i = 0; i < concurrency; i++) {
			reqExec.execute(() -> {

				for (int j = 0; j < reqSize; j++) {
					try {
						//                        counter.increment();
						executeRunnableCommand("GET_" + loadData.getLabel(),
								new ProfileMe() {

							@Override
							public void execute() throws Exception {
								try {
									String key = store.getAnyId();
									RequestOptions opts = new RequestOptions();
									opts.setPartitionKey(new PartitionKey(key));
									ResourceResponse<Document> response = client.readDocument(loadData.getDocumentLink(key), opts);
									if(!validate.validateGet(key,response.getResource())){
										throw new RuntimeException("Invalid Get Response");
									}
								} finally {
									counter.decrement();
								}
							}
						});
					} catch (Exception e) {
						e.printStackTrace(System.err);
						System.out.println(e.getMessage());
						failedCounter.increment();
					}
				}
			});
		}

	}

	@Override
	public void waitTillDone() throws InterruptedException {
		Thread.sleep(2000);
		//        System.out.println("GET " + counter.intValue());
		while (counter.intValue() != 0) {
			Thread.sleep(2000);
			//            System.out.println("GET " + counter.intValue());
		}

		System.out.println("Summary GET reqSize" + reqSize + " concurrency"
				+ concurrency + " failed count " + failedCounter.longValue());
	}
}
