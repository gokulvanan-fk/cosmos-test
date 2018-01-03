package com.flipkart.cosmosdb.load;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.cosmosdb.load.data.LoadData;
import com.flipkart.cosmosdb.load.profiler.ProfileContainer;
import com.flipkart.cosmosdb.load.runner.GetIndexLoad;
import com.flipkart.cosmosdb.load.runner.GetLoad;
import com.flipkart.cosmosdb.load.runner.LoadRunner;
import com.flipkart.cosmosdb.load.utils.ArchiveLogger;
import com.flipkart.cosmosdb.load.utils.EmptyValidator;
import com.flipkart.cosmosdb.load.utils.Store;
import com.flipkart.cosmosdb.load.utils.Validate;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;

public class RandomReadTest {

	
	static ObjectMapper objectMapper = new ObjectMapper();
	public static void main(String[] args) throws Exception {
		System.out.println("Starting randomRead Test");
		String profileLog = args[0];
		String loadProps = args[1];
		String archiveLog = args[2];

		Properties props = new Properties();	
		try (FileInputStream fio = new FileInputStream(new File(loadProps))) {
			props.load(fio);
		}

		LoadData loadData = LoadData.getLoader(props
				.getProperty("loadDataClass"));
		boolean isSequential = "sequential".equals(StringUtils.strip(props.getProperty("mode","concurrent")));
		//	        int noOfConnections = Integer.parseInt(props
		//	                .getProperty("hbasePoolSize"));


		long getReqSize = Long.parseLong(props.getProperty("GETReqSize", "0"));
		int getConcc = Integer.parseInt(props.getProperty("GETConcc", "1"));

		int getIndexReqSize = Integer.parseInt(props.getProperty("GET_INDEXReqSize", "0"));
		int getIndexConcc = Integer.parseInt(props.getProperty("GET_INDEXConcc", "1"));


		int threadPoolSize = getConcc  + getIndexConcc;
		if (isSequential)
			threadPoolSize = Math.max(getConcc, getIndexConcc);

		String consistencyLevel = props.getProperty("consistencyLevel", "Strong");

		int poolSize = Integer.parseInt(props.getProperty("poolSize", "1000"));
		ConsistencyLevel level = ConsistencyLevel.valueOf(consistencyLevel);

		ProfileContainer.startReporter(profileLog);

	
		
		LoaderSetup.INSTANCE.setup(poolSize,level, threadPoolSize,
				loadData);
		ExecutorService reqExec = LoaderSetup.INSTANCE.getReqExec();
		DocumentClient client = LoaderSetup.INSTANCE.getConn();
		
		Store store = ArchiveLogger.readSetup(archiveLog,threadPoolSize);

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

		LoadRunner getIndexLoad = null;
		if (getIndexReqSize > 0) {
			getIndexLoad = new GetIndexLoad(reqExec, client, store, validate, getIndexReqSize,
					getIndexConcc, loadData);
			getIndexLoad.runLoad();
			if (isSequential)
				getIndexLoad.waitTillDone();
		}

		
		if (getLoad != null)
			getLoad.waitTillDone();
		
		if (getIndexLoad != null)
			getIndexLoad.waitTillDone();
		
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
