export PARSER_HOME=`dirname "$this"`

echo 'Performing step3'

java -cp $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar com.yahoo.ycsb.Replay -db com.yahoo.ycsb.InfinispanGlue -db_path $PARSER_HOME/dbs -P $PARSER_HOME/file_workloads/workload_1 

echo 'Thats all Folks!'
