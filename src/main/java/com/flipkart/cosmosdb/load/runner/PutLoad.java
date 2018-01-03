package com.flipkart.cosmosdb.load.runner;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.LongAdder;

import com.flipkart.cosmosdb.load.data.LoadData;
import com.flipkart.cosmosdb.load.profiler.ProfileMe;
import com.flipkart.cosmosdb.load.utils.RandomUtil;
import com.flipkart.cosmosdb.load.utils.Store;
import com.microsoft.azure.documentdb.DocumentClient;

public class PutLoad implements LoadRunner {
	private final ExecutorService reqExec;
	private final long reqSize;
	private final int concurrency;
	private final DocumentClient client;
	private final LongAdder counter;
	private final LongAdder failedCounter = new LongAdder();
	private final Optional<Store> storeOpt;
	private final LoadData loadData;

	public PutLoad(ExecutorService exec, DocumentClient client, Store store,
			long reqSize, int conc, LoadData loadData) {
		this.storeOpt = Optional.ofNullable(store);
		this.reqExec = exec;
		this.reqSize = reqSize;
		this.concurrency = conc;
		this.client = client;
		this.loadData = loadData;
		this.counter = new LongAdder();
		this.counter.add(reqSize*concurrency);
	}

	@Override
	public void runLoad() throws InterruptedException {
		System.out.println("running Put load req " + reqSize + " concurrency "
				+ concurrency);
		for (int i = 0; i < concurrency; i++) {
			reqExec.execute(() -> {

				for (int j = 0; j < reqSize; j++) {
					try {
						//                        counter.increment();
						executeRunnableCommand("PUT_" + loadData.getLabel(),
								new ProfileMe() {
							@Override
							public void execute() throws Exception {

								try {
									String keyStr = RandomUtil.INSTANCE
											.randomKey();
									Object document = loadData
											.buildObject(keyStr);
									if(storeOpt.isPresent()){
										storeOpt.get().save(keyStr,document);
									}
									client.createDocument(loadData.getCollectionLink(),
											document, null, true);

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
		//        System.out.println("PUT " + counter.intValue());
		while (counter.intValue() != 0) {
			Thread.sleep(2000);
			//            System.out.println("PUT " + counter.intValue());
		}
		System.out.println("Summary PUT reqSize" + reqSize + " concurrency"
				+ concurrency + " failed count " + failedCounter);
	}
}
