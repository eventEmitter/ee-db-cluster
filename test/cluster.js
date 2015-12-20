(function() {
    'use strict';


    var   Class                 = require('ee-class')
        , log                   = require('ee-log')
        , assert                = require('assert')
        , QueryContext          = require('related-query-context')
        ;



    var   Cluster               = require('../')
        , config
        ;



    config = {
          db: 'test'
        , schema: 'related_db_cluster'
        , maxConnections: 10
    };




    // try to load the local test config
    try {
        let localConfig = require('../test-config.js');

        for (let key in localConfig) config[key] = localConfig[key];
    } catch(e) {}

    




    describe('The Cluster', function() {
        it('should not crash when instantiated', function() {
            new Cluster({driver: 'postgres'});
        });



        it('should be able to load a node', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});

            cluster.addNode(config).then(done).catch(done);
        });




        it('should be able to describe the db', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});

            cluster.addNode(config).then(() => {

                return cluster.describe(['related_db_cluster']).then((description) => {
                    assert(description);
                    done();
                });
            }).catch(done);
        });




        it('should be able to execute a query', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});

            cluster.addNode(config).then(() => {
                let qc = new QueryContext({
                      sql: 'INSERT INTO related_db_cluster.test (id) VALUES (default) RETURNING id;'
                    , pool: 'write'
                });

                return cluster.query(qc).then((data) => {
                    assert(data && data.id);
                    done();
                });
            }).catch(done);
        });



        it('should be able to insert 1000 items', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});

            cluster.addNode(config).then(() => {
                return Promise.all(Array.apply(null, {length: 1000}).map(() => {
                    return cluster.query(new QueryContext({
                          sql: 'INSERT INTO related_db_cluster.test (id) VALUES (default) RETURNING id;'
                        , pool: 'write'
                    })).then((data) => {
                        assert(data && data.id);
                        return Promise.resolve();
                    });
                })).then(() => {
                    done();
                });               
            }).catch(done);
        });



        it('should be able render a query', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});


            let context = new QueryContext({
                query: {
                      select: ['*']
                    , from: 'test'
                    , database: 'related_db_cluster'
                    , mode: 'select'
                    , filter: {
                        id: function() {
                            return {
                                  operator: '>'
                                , value: 1
                            };
                        }          
                    }
                }
                , pool: 'write'
            });


            cluster.addNode(config).then(() => {
                return cluster.query(context).then((data) => {
                    assert(data && data.length);
                    done();
                });              
            }).catch(done);
        });



        it('should be able compile an ast based query', function(done) {
            this.timeout(10000);

            let cluster = new Cluster({driver: 'postgres'});

            let context = new QueryContext({
                ast: {
                    kind: 'selectQuery'
                    , select: {
                          kind: 'select'
                        , selection: ['*']
                    }
                    , from: {
                          kind: 'from'
                        , entity: 'test'
                        , database: 'related_db_cluster'
                    } 
                }
                , pool: 'write'
            });


            cluster.addNode(config).then(() => {
                return cluster.query(context).then((data) => {
                    assert(data && data.length);
                    done();
                });              
            }).catch(done);
        });




        it('should recover correctly from failed inserts', function(done) {
            this.timeout(100000);

            let cluster = new Cluster({driver: 'postgres'});

            config.maxConnections = 5;

            cluster.addNode(config).then(() => {

                return Promise.all(Array.apply(null, {length: 100}).map(() => {
                    return cluster.getConnection('write').then((conn) => {

                        return conn.createTransaction().then(() => {
                            return conn.query(new QueryContext({
                                  sql:'INSERT nope;'
                                , pool: 'write'
                            })).then(() => {
                                return Promise.resolve();
                            }).catch((err) => {
                                //log.info('rolling back');
                                conn.rollback();
                                return Promise.reject(err);
                            });
                        });
                    }).then(() => {
                        return Promise.resolve();
                    }).catch((err) => {
                        //log(err);
                        return Promise.resolve();
                    });
                })).then((results) => {
                    done();
                });
            }).catch(done);
        });
    });    
})();
