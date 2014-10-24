export PARSER_HOME=`dirname "$this"`

echo 'currently parsing file: '
for f in $PARSER_HOME/ReplayLogs/*; do
  echo $f
  zcat $f | java -cp  $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar com.yahoo.ycsb.workloads.Parser step1 $PARSER_HOME/dbs $@
done
echo 'Thats all Folks!'
