package com.flipkart.cosmosdb.load;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.cosmosdb.load.data.LoadData;
import com.flipkart.cosmosdb.load.profiler.ProfileContainer;
import com.flipkart.cosmosdb.load.runner.GetLoad;
import com.flipkart.cosmosdb.load.runner.LoadRunner;
import com.flipkart.cosmosdb.load.runner.PutLoad;
import com.flipkart.cosmosdb.load.utils.ArchiveLogger;
import com.flipkart.cosmosdb.load.utils.EmptyValidator;
import com.flipkart.cosmosdb.load.utils.Store;
import com.flipkart.cosmosdb.load.utils.StoreImpl;
import com.flipkart.cosmosdb.load.utils.Validate;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

public class LoadTest {

	
	static ObjectMapper objectMapper = new ObjectMapper();
	public static void main(String[] args) throws Exception {
		System.out.println("Starting load Test");
		String profileLog = args[0];
		String loadProps = args[1];
		String archiveLog = args[2];

		Properties props = new Properties();	
		try (FileInputStream fio = new FileInputStream(new File(loadProps))) {
			props.load(fio);
		}

		LoadData loadData = LoadData.getLoader(props
				.getProperty("loadDataClass"));
		boolean isSequential = "sequential".equals(props.getProperty("mode"));
		//	        int noOfConnections = Integer.parseInt(props
		//	                .getProperty("hbasePoolSize"));

		int warmUpReqSize = Integer.parseInt(props.getProperty("warmupPutSize",
				"0"));
		int warmUpConcc = Integer.parseInt(props
				.getProperty("warmupConcc", "1"));

		long getReqSize = Long.parseLong(props.getProperty("GETReqSize", "0"));
		int getConcc = Integer.parseInt(props.getProperty("GETConcc", "1"));

		int putReqSize = Integer.parseInt(props.getProperty("PUTReqSize", "0"));
		int putConcc = Integer.parseInt(props.getProperty("PUTConcc", "1"));


		int threadPoolSize = Math.max(getConcc  + putConcc, warmUpConcc);
		if (isSequential)
			threadPoolSize = Math.max(Math.max(getConcc, putConcc),warmUpConcc);

		String consistencyLevel = props.getProperty("consistencyLevel", "Strong");

		int poolSize = Integer.parseInt(props.getProperty("poolSize", "1000"));
		ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel);

		ProfileContainer.startReporter(profileLog);

		ArchiveLogger.writeSteup(archiveLog);
		
		LoaderSetup.INSTANCE.setup(poolSize,level, threadPoolSize,
				loadData);
		ExecutorService reqExec = LoaderSetup.INSTANCE.getReqExec();
		DocumentClient client = LoaderSetup.INSTANCE.getConn();
		Store store = new StoreImpl((warmUpReqSize*(warmUpConcc)),threadPoolSize);

		if (warmUpReqSize > 0) {
			LoadRunner putLoad = new PutLoad(reqExec, client, store,
					warmUpReqSize, warmUpConcc, loadData);
			putLoad.runLoad();
			putLoad.waitTillDone();
		}

		Validate validate = new EmptyValidator(); 
		LoadRunner getLoad = null;
		if (getReqSize > 0) {
			int getMissPerc = 2; // TODO get from config
			getLoad = new GetLoad(reqExec, client, store,validate,getReqSize, getConcc,
					getMissPerc, loadData);
			getLoad.runLoad();
			if (isSequential)
				getLoad.waitTillDone();
		}

		LoadRunner secondPutLoad = null;
		if (putReqSize > 0) {
			secondPutLoad = new PutLoad(reqExec, client, null, putReqSize,
					putConcc, loadData);
			secondPutLoad.runLoad();
			if (isSequential)
				secondPutLoad.waitTillDone();
		}

		
		if (getLoad != null)
			getLoad.waitTillDone();
		
		if (secondPutLoad != null)
			secondPutLoad.waitTillDone();
		
		ArchiveLogger.close();
		LoaderSetup.INSTANCE.tearDown();
		System.exit(0);

	}

	static void wait5Seconds() {
		try {
			Thread.sleep(5*1000);
		}
		catch(InterruptedException e) {}
	}


}
