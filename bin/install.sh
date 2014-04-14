#!/bin/bash

source configuration.sh

SCPCMDNODE="scp -i id_rsa"

function listIP(){
    keyword=$1
    user=$2
    vms=`${SSHCMDHEAD} onevm list | grep ${user} | grep ${keyword} | cut -d " " -f 3`
    ips=()
    for vm in ${vms}
    do
	ip=`${SSHCMDHEAD} onevm show ${vm} | grep yes | awk '{print $5}'`
	ips+=(${ip})
    done
    echo ${ips[@]}
}

servers=(`listIP "multiversion-se" "psutra"`) 

let e=${#servers[@]}-1
for i in `seq 0 $e`
do
    ${SCPCMDNODE} ~/ispn.jar root@${servers[$i]}:/opt/infinispan/ispn.jar
done
