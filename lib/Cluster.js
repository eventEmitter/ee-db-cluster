!function() {
	'use strict';

	var   Class 			= require('ee-class')
		, log 				= require('ee-log')
		, EventEmitter 		= require('ee-event-emitter')
		, type 				= require('ee-types')
		, argv 				= require('ee-argv')
		, LinkedList 		= require('linkd')
		, Node 				= require('./Node')
		, ConnectionRequest	= require('./ConnectionRequest')
		;




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


		// ttl check interval
		, ttlCheckInterval: 30000


		// ttl
		, ttl: 60


		// max queue length
		, maxQueueLength: 10000


		// returns the length of all queues
		, queueLength: {
			get: function() {
				let l = 0;

				for (let queue of this.queues.values()) l += queue.length;

				return l;
			}
		}





		/**
		 * class constructor
		 *
		 * @param {object} options the configuration for the cluster
		 * @param {function} ConnectionConstructor the rdbms specific connection driver
		 */
		, init: function(options, ConnectionConstructor) {

			// validate
			if (!ConnectionConstructor) throw new Error('Missing the ConnectionConstructor!');


			// use the driver passed to the constructor
			this.ConnectionConstructor = ConnectionConstructor;



			if (options) {

				// the ttl can be set on the cluster
				if (options.ttl) this.ttl = options.ttl;

				// how many items may be queued
				if (options.maxQueueLength) this.maxQueueLength = options.maxQueueLength;
			}



			// storage for the queues
			this.queues = new Map();

			// create a map for fast queue access
			this.queueMap = new Map();

			// storage for the connections
			this.pools = new Map();

			// storage for nodes
			this.nodes = new Set();



			// check for the ttl regularly
			this.checkInterval = setInterval(this.executeTTLCheck.bind(this), this.ttlCheckInterval);
		}







		/**
		 * check for connection requests that are taking too long
		 */
		, executeTTLCheck: function() {
			for (let queue of this.queues.values()) {
				while (queue.length && queue.getLast().isExpired(this.ttl)) {
					let request = queue.shift();

					// cancel item
					request.abort(new Error('The connection request for a database connection timed out!'));

					// remove from other queues
					let queues = this.queueMap.get(request.pool);					
					for (let q of queues) {
						if (q.has(request.id)) q.remove(request.id);
					}
				}
			}
		}







		/**
		 * queues are used to store queries that cannot be executed 
		 * immerdiatelly. each db node has one or mode pool memberships,
		 * a queue is set up for each unique combination fo those memberships.
		 * Queries are put in each queue that has its specific pool registred 
		 * on it. this makes it possible that when a free connection is 
		 * available the best matching query can be executed on that connection.
		 *
		 * since queries are potentially stored in multiple queues it will be 
		 * removed from all queues as soon its beeing executed. using this 
		 * algorithm the pool should always be as balanced as possible.
		 *
		 * @param {node} node the db node
		 */
		, setUpQueue: function(node) {


			// get the pool composite name
			let compositeName = node.pools.join('/');


			// create a queue for the composite name. we use linked lists 
			// because we need O(1) access to the items in a queue since 
			// queries can be part of many queues.
			if (!this.queues.has(node.compositeName)) {
				let list = new LinkedList();

				// we need to count how many hosts are using this queue
				list.nodeCount = 0;

				// add as queue
				this.queues.set(node.compositeName, list);
			}


			// we'll need this later
			let queue = this.queues.get(node.compositeName);


			// we need to count how many hosts are using this queue
			queue.nodeCount++;


			// create a pool name based map so that wen can access the queues
			// fast (O(1))
			node.pools.forEach((poolName) => {

				// set up the node
				if (!this.queueMap.has(poolName)) this.queueMap.set(poolName, new Set());

				// add the queue itself to the set
				this.queueMap.get(poolName).add(queue);
			});





			// cleanup after hosts shutdown
			node.once('end', () => {

				// there should be a queue for this host, make sure its there!
				if (this.queues.has(node.compositeName)) {
					let list = this.queues.get(node.compositeName);

					list.nodeCount--;


					// the last node of this queue was removed, remove it!
					if (list.nodeCount === 0) {

						// go through all queries in the queue and checkif its contained
						// in any other queue, if no, return an error to the query callback
						for (let query of list) {
							if (this.queueMap.get(query.pool).length === 1) {
								// this is the last queue this query is part of
								// cancel it
								query.callback(new RelatedError.NoConncetionError('There is no suitable host left for the «'+query.pool+'» pool. The last host for that pool has gone down!'));
							}
						}


						// all queries where canceled, remove queue
						this.queues.delete(node.compositeName);


						// remove from maps
						node.pools.forEach((poolName) => {

							
							if (this.queueMap.get(poolName).length === 1) {

								// we're the last queue for this map item, removei it
								this.queueMap.delete(poolName);
							}
							else {
								
								// not the last item, remove this one from the set
								this.queueMap.get(poolName).delete(list);
							}
						});
					}
				}
				else throw new Error('Cannot remove «'+node.compositeName+'» queue, it does not exist!');
			});
		}










		/**
		 * sets up a connection pool for a given id
		 *
		  * @param {node} node the db node
		 */
		, setUpPool: function(node) {


			// make sure that we have a pool for every pool set on the node
			node.pools.forEach((poolName) => {
				if (!this.pools.has(poolName)) {

					// each pool is alinkedlist, used for fast acces
					let list = new LinkedList();

					// we need to count how many nodes add their connections
					// to this pool
					list.nodeCount = 0;

					// add to pool map
					this.pools.set(poolName, list);
				}


				// increase the node counter
				this.pools.get(poolName).nodeCount++;
			});



			// clean up the pools if the node gies down
			node.once('end', () => {

				node.pools.forEach((poolName) => {
					if (this.pools.has(poolName)) {
						let pool = this.pools.get(poolName);


						// decrease the node counf
						pool.nodeCount--;


						if (pool.nodeCount === 0) {

							// let us delte the pool, the connections should be 
							this.pools.delete(poolName);
						}
					}
					else throw new Error('Cannot remove «'+poolName+'» pool, it does not exist!');
				});				
			});
		}











		/**
		 * adds a new node to the cluster
		 *
		 * @param {object} configuration the node configuration
		 */
		, addNode: function(configuration) {
			if (this.ended) return Promise.reject(new Error('The cluster has ended, cannot add a new node!'));
			else {
				return new Promise((resolve, reject) => {
					// create node instance, set some sane defaults 
					// (this should work for most CIs)
					let node = new Node({
						  host 					: configuration.host || 'localhost'
						, username  			: configuration.username || configuration.user || 'postgres'
						, password  			: configuration.password || configuration.pass || ''
						, port 					: configuration.port || 5432
						, maxConnections 		: configuration.maxConnections || configuration.max || 100
						, pools 				: (configuration.pools || configuration.pool ? [configuration.pool] : ['read', 'write']).sort()
						, ConnectionConstructor : this.ConnectionConstructor
						, database 				: configuration.database || configuration.db || null
					});




					// set up a connection pool for the node
					this.setUpPool(node);

					// set up a queue for the node
					this.setUpQueue(node);





					// store the node so it can be accessed later
					this.nodes.add(node);					

					// if the node ends we need to handle that
					node.once('end', () => {
						this.nodes.delete(node);
					});





					// more importantly, if a node emits a connection it should be used for the
					// next connection request in the queue or added to the pool
					node.on('connection', (connection) => {


						// the connection may already be idle at this point
						if (connection.idle) this.handleIdleConnection(node, connection);


						// add the connection to the pools as soon its getting idle
						connection.on('idle', () => {
							 this.handleIdleConnection(node, connection);
						});
						

						// remove the connection from the pools as soon as
						// it ends
						connection.once('end', () => {
							for (let poolName of node.pools) {
								let p = this.pools.get(poolName);
								if (p.has(connection.id)) p.remove(connection.id);
							}
						});
					});
					


					node.once('load', resolve);
				});
			}
		}








		/**
		 * handles incoming idle connections
		 *
		 * @param {connection} the connection that has become idle
		 */
		, handleIdleConnection: function(node, connection) {

			// checl if we got a connection request wiating for us
			if (this.queues.has(node.compositeName) && this.queues.get(node.compositeName).length) {

				// we got a connection request to execute
				let request = this.queues.get(node.compositeName).shift();

				// lets now check the other compatible queues for 
				// the same connection request, remove it there
				for (let queue of this.queueMap.get(request.pool)) {
					if (queue.has(request.id)) queue.remove(request.id);
				}


				// return to the caller
				request.execute(connection);
			}
			else {
				
				// add the connection to the pools
				for (let poolName of node.pools) this.pools.get(poolName).push(connection.id, connection);
			}
		}













		/**
		 * request a connection from a specific pool
		 *
		 * @param {string} poolName the name of the pool
		 * 				   the connection must be from
		 *
		 * @returns {Promise} 
		 */
		, getConnection: function(poolName) {
			if (this.ended) return Promise.reject(new Error('The cluster has ended, cannot get connection!'));
			else if (this.pools.has(poolName) && this.pools.get(poolName).length) return Promise.resolve(this.pools.get(poolName).shift());
			else if (!this.queueMap.has(poolName) || !this.queueMap.get(poolName).size) return Promise.reject(new Error('Cannot get connection, no host is sesrving the pool «'+poolName+'»!'));
			else if (this.queueLength >= this.maxQueueLength) return Promise.reject(new Error('The connection queue is overflowing, request rejected!'));
			else {
				return new Promise((resolve, reject) => {

					// create a new connection request
					let request = new ConnectionRequest(poolName, resolve, reject);

					// get the set containg all relevant queues
					let queueSet = this.queueMap.get(poolName);

					// add to all relevant queues
					for (let queue of queueSet) queue.push(request.id, request);
				});
			}
		}









		/**
		 * executes a query on the next available db connection
		 *
		 * @param {object} query the query definition
		 *
		 * @returns {Promise}
		 */
		, query: function(query) {

			// first lets check the users input
			if (!type.object(query)) return Promise.reject(new Error('Expected a query definition object!'));
			else if (!type.string(query.pool)) return Promise.reject(new Error('Missing the pool property on the query definition!'));
			else {
				return this.getConnection(query.pool).then((connection) => {
					return connection.query(query);
				});
			}
		}










		/**
		 * ends the cluster
		 *
		 * @param {boolean} endNow aborts all queued queries
		 *
		 * @returns {Promise}
		 */
		, end: function(endNow) {
			if (this.ended) return Promise.reject(new Error('The cluster has ended, cannot end it again!'));
			else {
				// flag the end
				this.ended = true;


				// stop the ttl check
				clearInterval(this.checkInterval);



				// its asnyc
				return new Promise((resolve, reject) => {


					// do we have to abort all items in the queue?
					if (endNow) {

						// empty the queues
						for (let queueName of this.queues.keys()) {
							let queue = this.queues.get(queueName);

							for (let request of queue) request.abort(new Error('The cluster is shutting down, all queries are beeing aborted!'));

							// remove the queue
							this.queues.delete(queueName);
						}


						// end the nodes
						for (let node of this.nodes) node.end();


						// de-reference
						this.queues = null;


						// we're done
						resolve();
					}
					else {
						let keys = [];

						// missing the spread operator, but want ot be node v4 compatible :(
						for (let key of this.queues.keys()) keys.push(key);


						// wait for all queues to drain
						return Promise.all(keys.map((queueName) => {
							let queue = this.queues.get(queueName);

							if (!queue.length) return Promise.resolve();
							else {
								return new Promise((resolve, reject) => {
									queue.once('drain', resolve);
								});
							}
						})).then(() => {

							// end the nodes
							for (let node of this.nodes) node.end();


							// de-reference
							this.queues = null;

							// we're done
							return Promise.resolve();
						});

					}
				});
			}
		}
	});
}();
