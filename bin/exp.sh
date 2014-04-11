#!/bin/bash

source configuration.sh

function stopExp(){
    let e=${#clients[@]}-1
    for i in `seq 0 $e`
    do
        echo "stopping on ${clients[$i]}"
        ${SSHCMDNODE} ubuntu@${clients[$i]} "killall -SIGTERM java"
    done

}

trap "echo 'Caught Quit Signal'; stopExp; wait; exit 255" SIGINT SIGTERM

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
clients=(`listIP "multiversion-cl" "psutra"`) 

wikis="scn simple jap" # scn se
versioningTechniques="DUMMY ATOMICMAP TREEMAP" 
threads="25 50 75 100"

for wiki in ${wikis} 
do
    for versioningTechnique in ${versioningTechniques}
    do

        # echo "0. Settings"
	echo "${versioningTechnique} servers={${servers[@]}} clients={${clients}}"

	echo "1. Stopping ISPN instances"
	let e=${#servers[@]}-1
	for i in `seq 0 $e`
	do
    	    ${SSHCMDNODE} ${servers[$i]} killall java &> /dev/null & 
    	    # ${SSHCMDNODE} ${servers[$i]} /etc/local.d/02infinispan.stop &> /dev/null &
	done
	wait
	sleep 3

	echo "2. Starting ISPN instances"
	let e=${#servers[@]}-1
	for i in `seq 0 $e`
	do
    	    ${SSHCMDNODE} ${servers[$i]} /etc/local.d/02infinispan.start &> /dev/null &
	done
	wait
	sleep 20

	echo "3. Computing storage usage (before)"
	rm -f tmp3
	let e=${#servers[@]}-1
	for i in `seq 0 $e`
	do
    	    ${SSHCMDNODE} ${servers[$i]} "ps -ef|grep java | grep -v grep | tr -s \" \" | cut -d \" \" -f 2 | awk '{print \"/opt/oracle-jdk-bin-1.7.0.45/bin/jcmd \"\$0\" GC.class_histogram\" }' | sh 2>&1 | egrep ^Total | tr -s \" \"| cut -d \" \" -f 3" >> tmp3 &
	done
	wait
	avrgBeforeLoad=`${lua} stats.lua tmp3 | cut -f 1`
	stdgBeforeLoad=`${lua} stats.lua tmp3 | tr -s " " | cut -f 2`
	echo ${avrgBeforeLoad} ${stdgBeforeLoad}

	echo "4. Populating instances"
	rm -f tmp4
	CMD="${YCSB_DIR}/bin/ycsb.sh com.yahoo.ycsb.Client -s -load -threads 20 -db com.yahoo.ycsb.InfinispanGlue -p servers=\"${servers[@]}\" -p keys_file=${WIKI_DIR}/${wiki}.dat  -p replay_keys_file=${WIKI_DIR}/${wiki}.tr -P ${YCSB_DIR}/file_workloads/workload_1 -p oldid_file=${WIKI_DIR}/${wiki}.rev -p versioningTechnique=${versioningTechnique} -p debug=false -p clear=false"
	let e=${#clients[@]}-1
	for i in `seq 0 $e`
	do
    	    ${SSHCMDNODE} ubuntu@${clients[$i]} ${CMD} &> tmp4 &
	done
	wait
	grep RunTime tmp4    
	sleep 20
	
	echo "5. Computing storage usage (after)"
	rm -f tmp5
	let e=${#servers[@]}-1
	for i in `seq 0 $e`
	do
    	    ${SSHCMDNODE} ${servers[$i]} "ps -ef|grep java | grep -v grep | tr -s \" \" | cut -d \" \" -f 2 | awk '{print \"/opt/oracle-jdk-bin-1.7.0.45/bin/jcmd \"\$0\" GC.class_histogram\" }' | sh 2>&1 | egrep ^Total | tr -s \" \"| cut -d \" \" -f 3" >> tmp5 &
	done
	wait
	avrgAfterLoad=`${lua} stats.lua tmp5 | cut -f 1`
	stdAfterLoad=`${lua} stats.lua tmp5 | tr -s " " | cut -f 2`    
	echo ${avrgAfterLoad} ${stdgAfterLoad}

	echo "6. Computing overhead "
	avrgOverhead=`echo "(${avrgAfterLoad} - ${avrgBeforeLoad})/(10^6)" | bc`
	stdOverhead=`echo "sqrt((${stdAfterLoad})^2 - (${stdgBeforeLoad})^2)/(10^6)" | bc`
	echo "Overhead is: ${avrgOverhead}MB  (std=${stdOverhead}MB)"    

        # echo "7. Running experiments on ${wiki} Wiki"
        # rm -f tmp7-*
	# for thread in ${threads}
	# do
    	#     echo "Using  ${thread} threads"
    	#     CMD="${YCSB_DIR}/bin/ycsb.sh com.yahoo.ycsb.Client -t -s -replay -threads ${thread} -db com.yahoo.ycsb.InfinispanGlue -p servers=\"${servers}\" -p keys_file=${WIKI_DIR}/${wiki}.dat  -p replay_keys_file=${WIKI_DIR}/${wiki}.tr -P ${YCSB_DIR}/file_workloads/workload_1 -p oldid_file=${WIKI_DIR}/${wiki}.rev -p versioningTechnique=${versioningTechnique} -p debug=false -p clear=false"
    	#     let e=${#clients[@]}-1
    	#     for i in `seq 0 $e`
    	#     do
    	# 	${SSHCMDNODE} ubuntu@${clients[$i]} ${CMD} &> tmp7-${thread} &
    	#     done
    	#     wait
    	#     grep -i "AverageLatency(ms)" tmp7-${thread}
    	#     grep -i "Throughput" tmp7-${thread}
	# done
	
    done

done