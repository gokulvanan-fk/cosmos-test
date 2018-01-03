package com.flipkart.cosmosdb.load.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ArchiveLogger {

	private static BufferedWriter  writer;
	private static BlockingQueue<Object> logs = new LinkedBlockingQueue<>(); // unbounded
	private static ExecutorService exec;

	
	public static Store readSetup(String logPath, int concc) throws IOException{
		int size = 0;
		try(FileReader fir = new FileReader(new File(logPath))){
			try(BufferedReader bir = new BufferedReader(fir)){
				bir.readLine();
				size++;
			}
		}
		Store store = new StoreImpl(size,concc); 
		try(FileReader fir = new FileReader(new File(logPath))){
			try(BufferedReader bir = new BufferedReader(fir)){
				String line = bir.readLine();
				String[] buff = line.split(",");
				String key = buff[0];
				String index = buff[1];
				store.save(key,index);
			}
		}
		return store;
	}
	
	public static void writeSteup(String logPath) throws IOException{
		writer = new BufferedWriter(new FileWriter(new File(logPath),true));
		exec = Executors.newSingleThreadExecutor( new ThreadFactory() {

			@Override
			public Thread newThread(Runnable r) {
				Thread th = new Thread(r, "archive_logger");
				th.setDaemon(true);
				return th;
			}
		});
		exec.execute(new Dumper(writer, logs));
	}



	public static void close() throws Exception{
		try{
			if(exec != null){
				exec.shutdown();
				exec.awaitTermination(10, TimeUnit.SECONDS);
			}
			
		}finally{
			if(writer != null){
				writer.flush();
				writer.close();
			}
		}

	}


	static class Dumper implements Runnable {

		private final BufferedWriter writer;
		private final BlockingQueue<Object> queue;

		public Dumper(BufferedWriter logger, BlockingQueue<Object> queue){
			this.writer = logger;
			this.queue = queue;
		}

		public void run(){
			try{
				while(true){
					Object order = queue.take();
					try{
						writer.write(order.toString());
						writer.write("\n");
					}catch(Exception e){
						System.err.println(e.getMessage());
					}
				}
			}catch(Exception e){
				System.err.print(e.getMessage());
			}
		}
	}


	public static void log(Object data) {
		if(writer == null) return;
		logs.add(data);
	}

}
