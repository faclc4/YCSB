YCSB - Wikipedia dump + traces
====
To compile YCSB:

ant 

To compile the glue class to Infinispan run:

javac -cp .:build/ycsb.jar:lib/* InfinispanGlue.java

You can have multiple InfinispanGlue classes, each one for each versioning strategy you want to test. 

All the dependencies are located in the /lib folder.

====
Pre-process stage:

Before executing the benchmark, you should have:

1. dump.obj: the parsed object of the wikipedia dump;
 
2. tracesX: the file containing the access traces.

To build the tracesX file you should first clean the traces file by performing the following command over the **access_traces** input file:

grep http://en.wikipedia.org/wiki/ **access_traces** | grep -v Special: | grep -v User_talk: | grep -v Image: | grep -v Category: | grep -v Wikipedia: | grep -v Wikipedia_talk: |grep -v Portal: | grep -v User: | grep -v Talk | grep -v Template_talk | grep -v ? | grep -v css | grep -v class | grep -v title= >> tracesX

To configure workload modify the workload file: 

/file_workloads/workload_1

====
**To LOAD data run:**

  ./bin/ycsb.sh com.yahoo.ycsb.Client -t -db InfinispanGlue -p keys_file=dump.obj -p replay_keys_file=tracesX -P file_workloads/workload_1
  
  This command will load data according to the Wikipedia dump, as fast as possible.
  

**There are 2 modes to RUN the workload:**

1. REGULAR: this is the standart YCSB operation, performing operations according to workload.

./bin/ycsb.sh com.yahoo.ycsb.Client -t -db InfinispanGlue -p keys_file=file_workloads/dump.obj -p replay_keys_file=file_workloads/tracesX -P file_workloads/workload_1

2. REPLAY: this mode replays the wikipedia traces as in the tracesX file.

./bin/ycsb.sh com.yahoo.ycsb.Client -t **-replay** -db InfinispanGlue -p keys_file=file_workloads/dump.obj -p replay_keys_file=file_workloads/tracesX -P file_workloads/workload_1

  speedup: just add the -speedup tag to the previous command.



