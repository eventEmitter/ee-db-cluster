# ee-db-cluster

DB agnostic db cluster implementation. The module manages connections to readonly, readwrite & writeonly db nodes. This module is used by the ee-orm / related module. 

## installation

    npm install ee-db-cluster

## Version History

- 0.1.4: fixed bug in the cluster node implementation which didnt remove conenctions from the idle counter when a connection gets closed. Added version history to the docs
- 0.2.0: removed weak driver dependency
- 0.3.0: the query methods takes now a query configuration object instead of multiple parameters