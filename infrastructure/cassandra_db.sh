#!/bin/bash


PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=cassandra-db

peg up cassandra_db.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} cassandra

peg service cassandra-db cassandra start
