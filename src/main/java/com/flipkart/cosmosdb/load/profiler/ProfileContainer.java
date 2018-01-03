package com.flipkart.cosmosdb.load.profiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class ProfileContainer {

	private static final MetricRegistry metrics = new MetricRegistry();
	private static ConsoleReporter reporter;
	private static PrintStream printStream;
	private final ProfileMe runner;
	private final String name;

	public ProfileContainer(String name, 
			ProfileMe runner){
		this.name = name;
		this.runner = runner;
	}

	public void execute() {
		Timer timer = metrics.timer(name);
		long start = System.currentTimeMillis();
		try{
			runner.execute();
		}catch(Exception e){
			e.printStackTrace();
			metrics.meter(name+"_failure").mark();
		}finally{
			timer.update(System.currentTimeMillis()-start, TimeUnit.MILLISECONDS);
		}
	}

	public synchronized static void  startReporter(String filePath) throws FileNotFoundException{
		printStream = new PrintStream(new File(filePath));
		reporter = ConsoleReporter.forRegistry(metrics)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.outputTo(printStream)
				.build();
		reporter.start(10, TimeUnit.SECONDS);
	}


	public synchronized static void stopReporter(){
		reporter.stop();
		printStream.close();
	}

}
