export PARSER_HOME=`dirname "$this"`

java -cp $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar com.yahoo.ycsb.Replayer -db com.yahoo.ycsb.InfinispanGlue -load -db_path $PARSER_HOME/dbEnvCH -P $PARSER_HOME/workload
