java -Xms4g -Xmx4G -XX:+UseG1GC -cp target/cosmos-test-1.0-SNAPSHOT.jar com.flipkart.cosmosdb.load.RandomReadTest profile.log load.properties  id.log &> log &
