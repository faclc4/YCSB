#!/bin/bash
export PARSER_HOME=`dirname "$this"`

SSHCMDNODE="ssh -l root -i id_rsa"

CMD="/etc/local.d/02infinispan.stop"
baseIP="172.16.0."
servers=(100 101 102 103 104 48 49 51 54 67 74 75 76 77 80 81 82 88 90 91 92 93 95 99)

echo "servers"
for ip in ${servers[@]}
do
        ${SSHCMDNODE} ${baseIP}$ip ${CMD} &> /dev/null &
done
