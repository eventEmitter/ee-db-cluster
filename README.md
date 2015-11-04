# related-db-cluster

DB agnostic db cluster implementation. The module manages connections to readonly, readwrite & writeonly db nodes. This module is used by the related module. 


### reserved pool names

- write / writeonly : accepts only writing queries
- readwrite: accepts all queries
- read / readonly: accepts only reading queries
- all other names accept only queries that are targeted to them

A sample setup consisting of a master that accepts reads too, and 
3 read replicas, from which only should used for querying statistics.

pools: ['readwrite', 'master']
pools: ['readonly']
pools: ['readonly']
pools: ['stats']

the readonly traffic is distributed the three pool hosts, the readwrite host
should get less readonly traffic since he has also to handle the write traffic.

{
      type           : 'postgres'
    , database       : 'test'
    , schema         : 'test'
    , user           : ''
    , pass           : ''
    , maxConnections : 20
    , hosts: [{
          host: ''
        , pool: ['read', 'write', 'master']
    }, {
          host: ''
        , pool: ['stats']
    }]
}





// add the autoscale plugin
orm.use(new AWSAutoScale({
      aki: ''
    , secret: ''
    , config: {
          master: 'eb-live'
        , maxConnections: 50
        , pools: ['read']
    }
}))