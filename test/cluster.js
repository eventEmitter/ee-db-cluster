(function() {
    'use strict';


    var   Class                 = require('ee-class')
        , log                   = require('ee-log')
        , assert                = require('assert')
        ;



    var   Cluster               = require('../')
        , PostgresConnection    = require('related-postgres-connection')
        , config
        ;



    config = {
          db: 'test'
        , maxConnections: 10
    };




    // try to load the local test config
    try {
        let localConfig = require('../test-config.js');

        for (let key in localConfig) config[key] = localConfig[key];
    } catch(e) {}

    




    describe('The Cluster', function() {
        it('should not crash when instantiated', function() {
            new Cluster(null, PostgresConnection);
        });



        it('should be able to load a node', function(done) {
            this.timeout(10000);

            let cluster = new Cluster(null, PostgresConnection);

            cluster.addNode(config).then(done).catch(done);
        });



        it('should be able to execute a query', function(done) {
            this.timeout(10000);

            let cluster = new Cluster(null, PostgresConnection);

            cluster.addNode(config).then(() => {
                return cluster.query({
                      SQL: 'INSERT INTO related_db_cluster.test (id) VALUES (default) RETURNING id;'
                    , pool: 'write'
                    , mode: 'insert'
                }).then((record) => {
                    assert(record && record.type && record.values && record.values.id);
                    done();
                });
            }).catch(done);
        });



        it('should be able to insert 1000 items', function(done) {
            this.timeout(10000);

            let cluster = new Cluster(null, PostgresConnection);

            cluster.addNode(config).then(() => {
                return Promise.all(Array.apply(null, {length: 1000}).map(() => {
                    return cluster.query({
                          SQL: 'INSERT INTO related_db_cluster.test (id) VALUES (default) RETURNING id;'
                        , pool: 'write'
                        , mode: 'insert'
                    }).then((record) => {
                        assert(record && record.type && record.values && record.values.id);
                        return Promise.resolve();
                    });
                })).then(() => {
                    done();
                });               
            }).catch(done);
        });
    });    
})();
