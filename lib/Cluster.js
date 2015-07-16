!function() {
	'use strict';

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments')
		, Queue 		= require('ee-ttl-queue')
		, argv 			= require('ee-argv')
		, Promise 		= (Promise || require('es6-promise').Promise)
		, Node 			= require('./Node');


	var   dev 			= argv.has('dev-db')
		, debug 		= argv.has('debug-db');



	module.exports = new Class({
		inherits: EventEmitter



		// the laoded driver
		, driver: null

		// ended flag
		, _ended: false



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


			if (this._ended) throw new Error('The cluster has ended, cannot add a new node!');


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

			// save for later use
			this.nodes[mode].push(node);


			if(dev) log('added «'+mode+'» node ...');

			// return the node, so it camn be ended by the user
			return node;
		}



		/**
		 * closes all connections, marks this cluster as dead
		 *
		 * @param <boolean> kill, remove all queries from the queue, defaults to false
		 * @param <function> optional callback
		 */
		, end: function(kill, callback) {

			// swap arguments?
			if (type.function(kill)) {
				callback = kill;
				kill = false;
			}


			if (callback) this._executeEnd(kill).then(callback).catch(callback);
			else return this._executeEnd(kill);
		}





		/**
		 * end all queries or wait for them to end
		 */
		, _executeEnd: function(kill) {

			// mark as ended
			this._ended = true;

			// kill all remaingin queries
			if (kill) {
				Object.keys(this.queue).forEach(function(queueName) {
					while (this.queue[queueName].length) {
						this.queue[queueName].get(new Error('The cluster has ended, cannot execute query!'));
					}
				});

				return this._endNodes();
			}
			else {
				return Promise.all(Object.keys(this.queue).map(function(queueName) {
					if (!this.queue[queueName].length) return Promise.resolve();
					else this.queue[queueName].on('drain', Promise.resolve);
				}.bind(this))).then(this._endNodes.bind(this));
			}
		}




		/**
		 * end all nodes, close all connections
		 */
		, _endNodes: function() {
			return Promise.all(Object.keys(this.nodes).map(function(type) {
				return Promise.all(this.nodes[type].map(function(node) {
					return node.end();
				}.bind(this)));
			}.bind(this)));
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
		 * the query method executes a query on the next available connection
		 *
		 * @param <Object> Query configuration
		 */
		, query: function(configuration) {

			if (this._ended) {
				if (configuration && configuration.callback) configuration.callback(new Error('The cluster has ended, cannot execute query!'));
				else throw new Error('The cluster has ended, cannot execute query!');
			}
			else {

				// get a conneciton from the pool
				this._getConnection(function(err, connection) {
					if (err) {
						if (configuration.callback) configuration.callback(err);
					}
					else connection.query(configuration);
				}.bind(this));
			}
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


			if (this._ended) callback(new Error('The cluster has ended, cannot get a connection!'));
			else {

				// queue request
				this._getConnection(readonly, function(err, connection) {
					if (err) callback(err);
					else {
						connection.removeFromPool();
						callback(null, connection);
					}
				}.bind(this));
			}
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
			if (this._ended) callback(new Error('The cluster has ended, cannot get description!'));
			else {
				this.getConnection(function(err, connection) {
					if (err) callback(err);
					else {
						connection.describe(databases, function(err, data) {
							callback(err, data);
							connection.close();
						}.bind(this));
					}
				}.bind(this));
			}
		}



		/**
		 * the getConnection method returns a connection when one becomes available
		 *
		 * @param <Function> callback
		 */
		, createTransaction: function(callback) {
			if (this._ended) callback(new Error('The cluster has ended, cannot create a transaction!'));
			else {
				this.getConnection(false, function(err, connection) {
					if (err) callback(err);
					else {
						connection.startTransaction();
						callback(null, connection);
					}
				}.bind(this));
			}
		}
	});
}();
