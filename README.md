YCSB - Wikipedia dump + traces
====
To compile YCSB:

```bash
mvn clean package assembly:single
```

To compile the glue class to Infinispan run:
```bash
javac -cp .:target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar InfinispanGlue.java
```

You can have multiple InfinispanGlue classes, each one for each versioning strategy you want to test.
The InfinispanGLue is just an example.

====
Pre-process stage:

Before executing the benchmark, you should have:

- dump.obj & dumRev.obj : the parsed objects of the wikipedia dump;

To build the dump.obj and dumpRev.obj files do:

```bash
java -cp target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar com.yahoo.ycsb.workloads.CompressedDumpParser  wikiDump.xml
```
- tracesX: the file containing the access traces.

To build the tracesX file you should first clean the traces file by performing the following command over the **access_traces** input file:

```bash
grep 'http://en.wikipedia.org/wiki/\|http://en.wikipedia.org/w/' currenttmp | grep -v Special: | grep -v Image: | grep -v Category: | grep -v Wikipedia: | grep -v User: | grep -v Talk: | grep -v User_talk: | grep -v css | grep -v .class | grep -v Wikipedia% | grep -v 'oldid= -' 
```

To configure workload modify the workload file: 

/file_workloads/workload_1

====
**To LOAD data run:**

```bash
  ./bin/ycsb.sh com.yahoo.ycsb.Client -load -db InfinispanGlue -p keys_file=file_workloads/dump.obj -p replay_keys_file=file_workloads/tracesX -p oldid_file=file_workloads/dumpRevs.obj -P file_workloads/workload_1
```  
  
  This command will load data according to the Wikipedia dump, as fast as possible.
  

**There are 2 modes to RUN the workload:**

- REGULAR: this is the standart YCSB operation, performing operations according to workload.

```bash
./bin/ycsb.sh com.yahoo.ycsb.Client -t -replay -db InfinispanGlue -p keys_file=file_workloads/dump.obj -p replay_keys_file=file_workloads/tracesX -p oldid_file=file_workloads/dumpRevs.obj -P file_workloads/workload_1
```

- REPLAY: this mode replays the wikipedia traces as in the tracesX file.

```bash
./bin/ycsb.sh com.yahoo.ycsb.Client -t -replay -db InfinispanGlue -p keys_file=file_workloads/dump.obj -p replay_keys_file=file_workloads/tracesX -p oldid_file=file_workloads/dumpRevs.obj -P file_workloads/workload_1
```

  speedup: You can adjust the speedup parameter by changing it in  the workload configuration file, currently at file_workloads/workload_1
  
  Multiple clients: Add **-threads N** do the previous commands. N: number of clients
