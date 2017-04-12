YCSB - Wikipedia dump + traces
====
To compile YCSB:

```bash
mvn clean package
```

====
Pre-process stage:
Execute Scripts: 
```bash
sh step1.sh, sh step2.sh, sh step3.sh
```
To execute the Load action:
```bash 
sh LoadDump.sh
```

To execute the Replay action:
Execute Script: 
```bash 
sh replay.sh 
```

To configure workload modify the workload file: 

/file_workloads/workload_1

  speedup: You can adjust the speedup parameter by changing it in  the workload configuration file, currently at file_workloads/workload_1
  
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
