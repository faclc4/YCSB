export PARSER_HOME=`dirname "$this"`

echo 'currently parsing file: '
for f in $PARSER_HOME/Dumps/*; do
  echo $f
  7z e -so $f | java -cp $PARSER_HOME/target/ycsb-Wiki_dumps_replay-jar-with-dependencies.jar pt.haslab.ycsb.workloads.Parser step2 $PARSER_HOME/dbs/ $@
done
echo 'Thats all Folks!'
