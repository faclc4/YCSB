export PARSER_HOME=`dirname "$this"`

echo 'Replaying'

java -cp $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar pt.haslab.ycsb.Replay -t -db pt.haslab.ycsb.InfinispanGlue -db_path $PARSER_HOME/dbs -P $PARSER_HOME/file_workloads/workload_1 

echo 'Thats all Folks!'
