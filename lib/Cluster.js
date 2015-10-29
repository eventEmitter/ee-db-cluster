!function() {
	'use strict';

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, TTLQueue 		= require('ttl-queue')
		, argv 			= require('ee-argv')
		, LinkedList 	= require('linkd')
		, Node 			= require('./Node');


	var   dev 			= argv.has('dev-db')
		, debug 		= argv.has('debug-db');






	/**
	 * this class represents a db cluster with multiuple nodes 
	 * that can be addressed by the user directly or automatically
	 * by the heuristics if the orm.
	 * it starts nodes and has a pool of connections it can execute 
	 * queries on
	 */ 





	module.exports = new Class({
		inherits: EventEmitter



		// the constructor for creating connections
		, ConnectionConstructor: null


		// an ended node does not accept new input
		, ended: false



		// reseved host ids that cannot be used by the user
		, reservedIds: {
			  readonly: true
			, writeonly: true
			, readwrite: true
		}







		/**
		 * class constructor
		 *
		 * @param {object} options the configuration for the cluster
		 * @param {function} ConnectionConstructor the rdbms specific connection driver
		 */
		, init: function(options, ConnectionConstructor) {


			// use the driver passed to the constructor
			this.ConnectionConstructor = ConnectionConstructor;


			// storage for the queues
			this.queues = {};


			// storage for the connections
			this.connections = {};


			// storage for nodes
			this.nodes = [];


			// set up the two basic queues
			this.setUpQueue('readonly');
			this.setUpQueue('writeonly');
		}









		/**
		 * sets up a query queue for a given id
		 *
		 * @param {string} id the id of the queue
		 */
		, setUpQueue: function(id) {

			// set up the query queue
			if (!this.queues[id]) this.queues[id] = new TTLQueue();
		}






		/**
		 * sets up a connection pool for a given id
		 *
		 * @param {string} id the id of the pool
		 */
		, setUpPool: function() {

			// set up the connection pool. its a linked list
			// so that connections can be removed easily
			if (!this.connections[id]) this.connections[id] = new LinkedList();
		}











		/**
		 * adds a new node to the cluster
		 *
		 * @param {object} configuration the node configuration
		 */
		, addNode: function(configuration) {
			if (this.ended) throw new Error('The cluster has ended, cannot add a new node!');

			// we need to check if the user tries to use one of the reserved
			// ids, whih cannot be manually assigned
			if (configuration.id && this.reservedIds[configuration.id]) throw new Error('The id of any host cannot be one of the following: '+Object.keys(this.reservedIds).join(', '));


			// make sure the node mode is valid
			if (configuration.mode && !this.reservedIds[configuration.mode]) throw new Error('A node configuration contains the mode «'+configuration.mode+'» which is not valid, only the following modes are valid: '+Object.keys(this.reservedIds).join(', '));



			// create node instance, set some sane defaults 
			// (this should work for most CIs)
			let node = new Node({
				  host 					: configuration.host || 'localhost'
				, username  			: configuration.username || configuration.user || 'postgres'
				, password  			: configuration.password || configuration.pass || ''
				, port 					: configuration.port || 5432
				, mode 					: configuration.mode || 'readwrite'
				, maxConnections 		: configuration.maxConnections || configuration.max || 100
				, idOnly 				: configuration.idOnly || false;
				, id 				 	: configuration.id || Symbol('anonymous')
				, ConnectionConstructor : this.ConnectionConstructor
			});



			// store the node so it can be accessed later
			this.nodes.push(node);

			
			// so, there are 3 generic modes (readonly, readwrite, writeonly)
			// and there are named hosts (id property). There is a query queue for
			// each mode and each id we encounter. The orm itself decides when 
			// to use a connection of which mode. The user may override this using 
			// the .host() method on querybuilders.
			this.setUpPool(configuration.mode);

			// the id based queue
			if (configuration.id) this.setUpQueue(configuration.id);
			if (configuration.id) this.setUpPool(configuration.id);





			// if the node ends we need to handle that
			node.once('end' () => {
				let idx = this.nodex.indexOf(node);

				// remove node from node list
				if (idx >= 0) this.nodes.splice(idx, 1);
			});




			// more importantly, if a node emits a connection it should be used for the
			// next query in the queue or added to the pool
			node.on('connection', (connection) => {



				// add the connection to the pools as soon its getting idle
				connection.on('idle', () => {

					// try to execute immediatelly
					if (!this.executeQuery(connection)) {

						// if the host has a user defined id add it to that pool
						if (connection.hasHostId) this.connections[connection.hostId].push(connection.id, connection);

						// add it to the mode pool
						this.connections[connections.mode].push(connection.id, connection);
					}
				});
				


				// remove the connection from the pools as soon as
				// it ends
				connection.once('end', () => {

					// if the host has a user defined id remove it from that pool
					if (connection.hasHostId) this.connections[connection.hostId].remove(connection.id);

					// remove it from the mode pool
					this.connections[connections.mode].remove(connection.id);
				});
			});
		}











		/**
		 * this method is called if a connection is getting free
		 *
		 * @param {object} connection the connection that became available
		 */
		, executeQuery: function(connection) {

			// check which queue to work on
			if (connection.host.isIdQuery && this.queues[connection.host.id].length) return this.queues[connection.host.id].get()(connection), true;
			else {
				// execute a mode based query
				let advisedMode = connection.host.queryMode;


				// nice, we got a match, execute the query
				if (this.queues[advisedMode].length) return this.queues[advisedMode].get()(connection), true;
				else {

					// checck the other option
					advisedMode = connection.host.queryMode;

					// nice, we got a match, execute the query
					if (this.queues[advisedMode].length) return this.queues[advisedMode].get()(connection), true;
				}
			}


			// the connection will be returned to the pool
			return false;
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
			var connection;


			// readonly stuff
			if (this.queue.readonly.length) {
				if (this.connections.readonly.length) {
					connection = this.connections.readonly.shift();
					this.queue.readonly.get()(null, connection);
					if(dev) log('returned «readonly» connection for a readonly query ...');
				}
				else if (this.connections.readwrite.length) {
					connection = this.connections.readwrite.shift();
					this.queue.readonly.get()(null, connection);
					if(dev) log('returned «readwrite» connection for a readonly query ...');
				}

				if (connection) {

					// check if the connection is also part of any other collection
					this.hosts.forEach(function(id) {
						var idx = this.connections[id].indexOf(connection);
						if (idx >= 0) this.connections[id].splice(idx, 1);
					}.bind(this));

					connection = null;
				}
			}

			// writing stuff
			if (this.queue.readwrite.length) {
				if (this.connections.writeonly.length) {
					connection = this.connections.writeonly.shift();
					this.queue.writeonly.get()(null, connection);
					if(dev) log('returned «writeonly» connection for a writing query ...');
				}
				else if (this.connections.readwrite.length) {
					connection = this.connections.readwrite.shift();
					this.queue.readwrite.get()(null, connection);
					if(dev) log('returned «readwrite» connection for a writing query ...');
				}

				if (connection) {

					// check if the connection is also part of any other collection
					this.hosts.forEach(function(id) {
						var idx = this.connections[id].indexOf(connection);
						if (idx >= 0) this.connections[id].splice(idx, 1);
					}.bind(this));

					connection = null;
				}
			}


			
			// host specific stuff
			this.hosts.forEach(function(id) {
				if (this.queue[id].length) {
					if (this.connections[id].length) {
						connection = this.connections[id].shift();
						this.queue[id].get()(null, connection);
						if(dev) log('returned «'+id+'» connection for a '+id+' query ...');
					}
				}

				if (connection) {

					// check if the connection is also part of any other collection
					['readonly', 'readwrite', 'writeonly'].forEach(function(id) {
						var idx = this.connections[id].indexOf(connection);
						if (idx >= 0) this.connections[id].splice(idx, 1);
					}.bind(this));
				}
			}.bind(this));
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
				this._getConnection(configuration.host, !configuration.readOnly, function(err, connection) {
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
		 * @param {string} the host id, optional
		 */
		, getConnection: function() {
			var   readonly 	= true
				, i  		= arguments.length
				, hostId 	= null
				, callback;

			while(i--) {
				if (typeof arguments[i] === 'function') callback = arguments[i];
				else if (typeof arguments[i] === 'boolean') readonly = arguments[i];
				else if (typeof arguments[i] === 'string') hostId = arguments[i];
			}


			if (this._ended) callback(new Error('The cluster has ended, cannot get a connection!'));
			else {

				// queue request
				this._getConnection(readonly, hostId, function(err, connection) {
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
				, hostId 	= null
				, callback;

			while(i--) {
				if (typeof arguments[i] === 'function') callback = arguments[i];
				else if (typeof arguments[i] === 'boolean' && arguments[i] === false) mode = 'readwrite';
				else if (typeof arguments[i] === 'string') hostId = arguments[i];
			}


			// queue request
			if (hostId && !this.queue[hostId]) return callback(new Error('Failed to add the connection request to the queue, the host «'+hostId+'» does not exist!').setName('InvalidHostException'));
			if (!this.queue[hostId || mode].add(callback)) return callback(new Error('Failed to add the connection request to the queue, overflow!').setName('OverflowException'));

			if(dev) log('added «'+(hostId || mode)+'» connection request to queue ...');

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
