(function() {
    'use strict';


    var   Class                 = require('ee-class')
        , log                   = require('ee-log')
        , assert                = require('assert');



    var   Cluster               = require('../')
        , PostgresConnection    = require('related-postgres-connection')
        , config = {};





    // try to load the local test config
    try {
        config = require('../test-config.js')
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
    });    
})();
