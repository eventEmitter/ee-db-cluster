# ee-db-cluster

DB agnostic db cluster implementation. The module manages connections to readonly, readwrite & writeonly db nodes. This module is used by the ee-orm module. 

## DB Connectors

- MySQL: ee-mysq-connection 

## installation

    npm install ee-db-cluster

## build status

[![Build Status](https://travis-ci.org/eventEmitter/ee-db-cluster.png?branch=master)](https://travis-ci.org/eventEmitter/ee-db-cluster)


## Version History

- 0.1.4: fixed bug in the cluster node implementation which didnt remove conenctions from the idle counter when a connection gets closed. Added version history to the docs