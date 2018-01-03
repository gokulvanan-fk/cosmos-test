package com.flipkart.cosmosdb.load.runner;

import com.flipkart.cosmosdb.load.profiler.ProfileContainer;
import com.flipkart.cosmosdb.load.profiler.ProfileMe;


public interface LoadRunner {

    public default <T> void executeRunnableCommand(String name,
            ProfileMe runner) {
      
        new ProfileContainer(name,runner).execute();

    }

    void runLoad() throws InterruptedException;

    void waitTillDone() throws InterruptedException;
}
