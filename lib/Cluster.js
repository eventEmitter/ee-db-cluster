!function() {
	'use strict';

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments')
		, Queue 		= require('ee-ttl-queue')
		, argv 			= require('ee-argv')
		, Node 			= require('./Node');


	var   dev 			= argv.has('dev-db')
		, debug 		= argv.has('debug-db');



	module.exports = new Class({
		inherits: EventEmitter



		// the laoded driver
		, driver: null


		/**
		 * class constructor
		 *
		 * @param <Object> options
		 */
		, init: function(options, driver) {

			// map of all connected nodes
			this.nodes = {
				  readonly: 	[]
				, readwrite: 	[]
				, writeonly: 	[]
			};

			// map of all connections
			this.connections = {
				  readonly: 	[]
				, readwrite: 	[]
				, writeonly: 	[]
			};

			// request queues
			this.queue = {};

			// writeonly nodes?
			this.allowReadsOnWritabelNodes = !!options.allowReadsOnWritabelNodes;

			// use the driver passed to the constructor
			this.driver = driver;

			// create queue infrastructure
			this._createQueue('readonly');
			this._createQueue('readwrite');
		}



		/**
		 * the _createQueue initilaizes the queue system
		 */
		, _createQueue: function(id) {
			// create connection queues
			this.queue[id] = new Queue({
				  ttl: 10000 	// don't wait longer than 10 seconds before aborting a query
				, max: 10000 	// don't queue more than 10k queries
			});

			this.queue[id].on('timeout', function(callback) {
				callback(new Error('Failed to get a connection!'));
			}.bind(this));

			this.queue[id].on('error', function() {
				log.warn('DB connection queue is overflowing ...');
			}.bind(this));
		}



		/**
		 * the addNode method lets the user add a node to the cluster. a node may be of any 
		 *
		 * @param <Object> options -> db credentials
		 * @param <String> mode of the node 'readonly', 'readwrite', 'writeonly', defualts to
		 * 				   readonly
		 */
		, addNode: function() {
			var   options 	= arg(arguments, 'object', {})
				, mode 		= arg(arguments, 'string', options.mode || 'readonly')
				, node;


			// create node instance
			node = new Node(options, this.driver, mode);

			// remove ended nodes
			node.on('end', function() {
				var index = this.nodes[mode].indexOf(node);
				if (index >= 0) this.nodes[mode].splice(index, 1);
				if(dev) log('removed «'+mode+'» node ...');
			}.bind(this));

			// emit connection
			node.on('connection', function(connection) {
				if(dev) log('got free «'+mode+'» connection ...');

				this.connections[mode].push(connection);

				// remove connection from pool when closed
				connection.once('end', function() {
					var index = this.connections[mode].indexOf(connection);
					if (index >= 0) this.connections[mode].splice(index, 1);
					//selse log.error('failed to remove connection from pool!');

					if(dev) log('«'+mode+'» connection has ended, removed connection at index «'+index+'»...');
				}.bind(this));

				// add idling connections to pool of available connections
				connection.on('idle', function() {
					if (dev) log('got idling «'+mode+'» connection ...');
					this.connections[mode].push(connection);
					this._execute();
				}.bind(this));

				this._execute();
			}.bind(this));

			if(dev) log('added «'+mode+'» node ...');

			// return the node, so it camn be ended by the user
			return node;
		}




		/**
		 * the _execute method tries to get items from the queues so they can be executed
		 */
		, _execute: function() {

			// readonly stuff
			if (this.queue.readonly.length) {
				if (this.connections.readonly.length) {
					this.queue.readonly.get()(null, this.connections.readonly.shift());
					if(dev) log('returned «readonly» connection for a readonly query ...');
				}
				else if (this.connections.readwrite.length) {
					this.queue.readonly.get()(null, this.connections.readwrite.shift());
					if(dev) log('returned «readwrite» connection for a readonly query ...');
				}
			}

			// writing stuff
			if (this.queue.readwrite.length) {
				if (this.connections.writeonly.length) {
					this.queue.writeonly.get()(null, this.connections.writeonly.shift());
					if(dev) log('returned «writeonly» connection for a writing query ...');
				}
				else if (this.connections.readwrite.length) {
					this.queue.readwrite.get()(null, this.connections.readwrite.shift());
					if(dev) log('returned «readwrite» connection for a writing query ...');
				}
			}
		}


		/**
		 * the query method executes a query on the next availabel connection
		 *
		 * @param <Object> Query configuration
		 */
		, query: function(configuration) {
			// get a conneciton from the pool
			this._getConnection(function(err, connection) {
				if (err) {
					if (configuration.callback) configuration.callback(err);
				}
				else connection.query(configuration);
			}.bind(this));
		}



		/**
		 * the getConnection method returns a connection when one becomes available.
		 * parameters may be passed in any order
		 *
		 * @param <Function> callback
		 * @param <Boolean> readonly connection, optional, defaults to true
		 */
		, getConnection: function() {
			var   readonly 	= true
				, i  		= arguments.length
				, callback;

			while(i--) {
				if (typeof arguments[i] === 'function') callback = arguments[i];
				else if (typeof arguments[i] === 'boolean') readonly = arguments[i];
			}

			// queue request
			this._getConnection(readonly, function(err, connection) {
				if (err) callback(err);
				else {
					connection.removeFromPool();
					callback(null, connection);
				}
			}.bind(this));
		}



		, _getConnection: function() {
			var   mode 		= 'readonly'
				, i  		= arguments.length
				, callback;

			while(i--) {
				if (typeof arguments[i] === 'function') callback = arguments[i];
				else if (typeof arguments[i] === 'boolean' && arguments[i] === false) mode = 'readwrite';
			}

			// queue request
			if (!this.queue[mode].add(callback)) callback(new Error('Failed to add the connection request to the queue, overflow!').setName('OverflowException'));

			if(dev) log('added «'+mode+'» connection request to queue ...');
			
			// trigger queue
			this._execute();
		}



		/**
		 * the describe method returns a description of all availabel databases
		 *
		 * @param <Function> callback
		 */
		, describe: function(databases, callback) {
			this.getConnection(function(err, connection) {
				if (err) callback(err);
				else connection.describe(databases, callback);
			}.bind(this));
		}



		/**
		 * the getConnection method returns a connection when one becomes available
		 *
		 * @param <Function> callback
		 */
		, createTransaction: function(callback) {
			this.getConnection(false, function(err, connection) {
				if (err) callback(err);
				else {
					connection.startTransaction();
					callback(null, connection);
				}
			}.bind(this));
		}
	});
}();
