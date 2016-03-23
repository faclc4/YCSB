export PARSER_HOME=`dirname "$this"`

echo 'Performing step3'

java -cp $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar com.yahoo.ycsb.workloads.Parser step3 $PARSER_HOME/dbs

echo 'Thats all Folks!'

