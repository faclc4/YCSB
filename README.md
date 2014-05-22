YCSB - Wikipedia dump + traces
====
To compile YCSB:

```bash
mvn clean package assembly:single
```

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
  ./bin/ycsb.sh com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.InfinispanGlue -p keys_file=file_workloads/en-wiki.dat -p replay_keys_file=file_workloads/en-wiki.tr -p oldid_file=file_workloads/en-dump.rev sizes_file=file_workloads/en.sizes -P file_workloads/workload_1
```  
  
  This command will load data according to the Wikipedia dump, as fast as possible.
  

**There are 2 modes to RUN the workload:**

- REGULAR: this is the standart YCSB operation, performing operations according to workload.

```bash
./bin/ycsb.sh com.yahoo.ycsb.Client -t -replay -db com.yahoo.ycsb.InfinispanGlue -p keys_file=file_workloads/en-wiki.dat -p replay_keys_file=file_work    loads/en-wiki.tr -p oldid_file=file_workloads/en-dump.rev sizes_file=file_workloads/en.sizes -P file_workloads/workload_1
```

- REPLAY: this mode replays the wikipedia traces as in the tracesX file.

```bash
./bin/ycsb.sh com.yahoo.ycsb.Client -t -replay -db com.yahoo.ycsb.InfinispanGlue -p keys_file=file_workloads/en-wiki.dat -p replay_keys_file=file_work    loads/en-wiki.tr -p oldid_file=file_workloads/en-dump.rev sizes_file=file_workloads/en.sizes -P file_workloads/workload_1
```

  speedup: You can adjust the speedup parameter by changing it in  the workload configuration file, currently at file_workloads/workload_1
  
  Multiple clients: Add **-threads N** do the previous commands. N: number of clients

====
**Infinispan Cluster operations**

The bin folder contains exp.sh, a helper to run YCSB in a distributed environment supporting the OpenNebula IaaS.
To configure it, we use the configuration.sh file.
In what follows, we first detail the content of configuration.sh then explain how to make use of exp.sh.

* Configuration 
In order to launch the experiments, there are mainly three modifications to do to configuration.sh. 
First, variables ${USER} and ${ONE_AUTH_FILE} should refer to the appropriate user name and OpenNebula authentificaiton files, and ${CLUSTER} should be modified to point to the cluster head.
Second, variable ${VM_SERVER}, ${VM_CLIENT} and ${VM_USER} should be set to resepectively the name of the server VMs, the name of the client VM and the name of the OpenNebula user that created these VMs. 

* Execution
The file bin/exp.sh is the one to use in order to launch a series of experiments.
This script file first retrieves the IP addresses of the servers and the client VMs.
Then, it runs the experiments according to the following parameters that respectively identify the wikipedias, the versioning techniques and the amount of threads that are used to execute the experiments.
```bash
wikis="scn" # en ja (simple se)
versioningTechniques="DUMMY TREEMAP" 
threads="10 25 50" # 75 100
```
An experiment consists in loading Infinispan with the appropriate versioning technique, executing a replay of the trace with a varying number of threads and computing the average and standard dev. of the storage cost. 
Each of this step outputs appropriate information to the console.
As a consequence, a convenient way to launch exp.sh is as follows:
```bash
nohup bin/exp.sh 2>&1 > results&
```
