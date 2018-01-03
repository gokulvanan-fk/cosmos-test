package com.flipkart.cosmosdb.load.runner;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

import com.flipkart.cosmosdb.load.data.LoadData;
import com.flipkart.cosmosdb.load.profiler.ProfileMe;
import com.flipkart.cosmosdb.load.utils.Store;
import com.flipkart.cosmosdb.load.utils.Validate;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponse;
import com.microsoft.azure.documentdb.SqlQuerySpec;

public class GetIndexLoad implements LoadRunner {
	private final ExecutorService reqExec;
	private final long reqSize;
	private final int concurrency;
	private final DocumentClient client;
	private final LongAdder counter = new LongAdder();
	private final LongAdder failedCounter = new LongAdder();
	private final Store store;
	private final LoadData loadData;
	private final Validate validate;

	public GetIndexLoad(ExecutorService exec, DocumentClient client, 
			Store store, Validate validate,
			long reqSize, int conc, LoadData loadData) {
		this.store = store;
		this.reqExec = exec;
		this.reqSize = reqSize;
		this.validate = validate;
		this.concurrency = conc;
		this.client = client;
		this.loadData = loadData;
		this.counter.add(reqSize*concurrency);
	}

	@Override
	public void runLoad() throws InterruptedException {
		System.out.println("running GetIndex load req " + reqSize + " concurrency "
				+ concurrency);
		for (int i = 0; i < concurrency; i++) {
			reqExec.execute(() -> {

				for (int j = 0; j < reqSize; j++) {
					try {
						//                        counter.increment();
						executeRunnableCommand("GET_INDEX_" + loadData.getLabel(),
								new ProfileMe() {

							@Override
							public void execute() throws Exception {
								try {
									String indexKey = (String) store.get(store.getAnyId());
									FeedOptions opts = new FeedOptions();
									opts.setEnableCrossPartitionQuery(true);
									SqlQuerySpec querySpec =  new SqlQuerySpec("select * from orders o where o.accountId='"+indexKey+"'");
									FeedResponse<Document> queryResponse = client.queryDocuments(loadData.getCollectionLink(), querySpec, opts);
									Iterator<Document> docs = queryResponse.getQueryIterator();
									while(docs.hasNext()){
										Document doc = docs.next();
										boolean valid = validate.validateGet(indexKey,doc);
										if(!valid){
											System.err.println("Got invalid doc for index "+indexKey+" doc "+doc.getId());
										}
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

		System.out.println("Summary GET_INDEX reqSize" + reqSize + " concurrency"
				+ concurrency + " failed count " + failedCounter.longValue());
	}
}
