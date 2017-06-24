#!/bin/bash


PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=hive-presto-secor

peg up hive_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
