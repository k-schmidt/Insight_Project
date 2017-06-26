#!/bin/bash


PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=kafka-cluster

peg up workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
