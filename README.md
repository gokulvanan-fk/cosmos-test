# cosmos-test
mvn clean package to build fat jar..  

update LoaderSetup.java with apiEndoint and key

use bash run.sh to run the Warmup Put load followed by GetLoad (Hot reads).

user bash readrun.sh to run RandomRead load along with index Get load. (Note: this works only if bash run.sh was invoked first to create id.log)

check profile.log to see metrics aggregate and flushed in 10 sec window

use load.properties to alter load.. requestSize => size per thread and concc = number of threads

