#!/bin/bash


PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=spark-cluster

peg up spark_master.yml &
peg up spark_workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark
