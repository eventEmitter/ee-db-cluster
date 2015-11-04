!function() {
    'use strict';

    var   Class             = require('ee-class')
        , log               = require('ee-log')
        ;






    module.exports = new Class({

        // the timestamp is used to check if 
        // the request is already qaiting too long
        created: Date.now()


        // each request has a unique id
        , id: null


        // flag if the request was answered already
        , answered: false


        /**
         * class constructor
         *
         * @param {string} pool the name of the pool this connection is for
         * @param {function} resolve, the callback for requests that fo through
         * @param {function} reject, tha callback for errors
         */
        init: function(pool, resolve, reject) {


            // update the created timestamp
            this.created = Date.now();

            // create an unique id
            this.id = Symbol();



            // the request must define for which pool it is
            this.pool = pool;

            // we need to store the callback for later
            this.resolve = resolve;
            this.reject = reject;
        }






        /**
         * invokes the request callback
         *
         * @param {connection} connection a db connection
         */
        , execute: function(connection) {
            if (!this.answered) {
                this.answered = true;
                this.resolve(connection);
            }
        }







        /**
         * invokes the request callback
         *
         * @param {connection} connection a db connection
         */
        , abort: function(err) {
            if (!this.answered) {
                this.answered = true;
                this.reject(err);
            }
        }







        /**
         * checks if the request has expired against a
         * given ttl
         *
         * @param {number} ttl the ttl in seconds
         */
        , isExpired: function(ttl) {
            return (Date.now() - ttl*1000) > this.created;
        }
    });
}();
