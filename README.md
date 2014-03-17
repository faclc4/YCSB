YCSB
====
To compile YCSB:

ant 

To compile the glue class to Infinispan run:

javac -cp .:build/ycsb.jar:lib/* InfinispanGlue.java

All the dependencies are located in the /lib folder.

====
To configure workload modify the workload file: 

/file_workloads/workload_1

====
To LOAD data run:

./bin/ycsb.sh com.yahoo.ycsb.Client -load -db InfinispanGlue -p keys_file=dump.txt -P file_workloads/workload_1

To RUN the workload:

./bin/ycsb.sh com.yahoo.ycsb.Client -t -db InfinispanGlue -p keys_file=dump.txt -P file_workloads/workload_1
