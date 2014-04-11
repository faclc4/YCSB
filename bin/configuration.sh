#!/bin/bash

YCSB_DIR="/home/ubuntu/YCSB"
WIKI_DIR="/home/ubuntu/wikipedia"

USER="psutra"
CLUSTER="clusterinfo.unineuchatel.ch"
ONE_AUTH_FILE="/home/psutra/one_auth"
ID_DATASTORE="100"

SSHCMDHEAD="ssh -l ${USER} ${CLUSTER} ONE_AUTH=${ONE_AUTH_FILE}"
SSHCMDNODE="ssh -l root -i id_rsa"

bc=`which bc`
lua=`which lua`
