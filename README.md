# related-db-cluster

DB agnostic db cluster implementation. The module manages connections to readonly, readwrite & writeonly db nodes. This module is used by the related module. 



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