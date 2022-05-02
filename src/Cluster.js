const logd = require('logd');
const LinkedList = require('linkd');
const Node = require('./Node.js');
const ConnectionRequest = require('./ConnectionRequest.js');
const Events = require('events');


const log = logd.module('Cluster');


/**
 * this class represents a db cluster with multiuple nodes 
 * that can be addressed by the user directly or automatically
 * by the heuristics if the orm.
 * it starts nodes and has a pool of connections it can execute 
 * queries on
 */ 




module.exports = class Cluster extends Events {


	// an ended node does not accept new input
	ended = false;


	// ttl check interval
	ttlCheckInterval = 30000;


	// ttl
	ttl = 60;


	// max queue length
	maxQueueLength = 10000;


    get queueLength() {
        let l = 0;
        for (let queue of this.queues.values()) l += queue.length;
        return l;
    }


	/**
	 * class constructor
	 *
	 * @param {object} options the configuration for the cluster
	 */
	constructor(options) {
		super();

		log.debug(`Loading the DB cluster ..`);

		// validate
		if (!options) throw new Error('The cluster expects an options object!');
		if (!options.driver) throw new Error('Please define which driver the cluster must load!');


		// currently postgres or mysql, they are included 
		// in this package using npm
		this.driverName = options.driver;
		log.debug(`The Cluster uses the ${this.driverName} driver`);

		
		// load vendor modules
		this.loadVendorModules();



		// the ttl can be set on the cluster
		if (options.ttl) this.ttl = options.ttl;

		// how many items may be queued
		if (options.maxQueueLength) this.maxQueueLength = options.maxQueueLength;
	

		// storage for the queues. Each queue is a linked list.
		// the key is the composite name of the pools it hosts, 
		// so «read» or «read, write». the linked list is used
		// to store the requests that are waiting for a connection.
		// linked lists are used becuae connection requests can be 
		// stored in multiple wueues («read» or «read, write») and
		// the items need to be removed from all queues in a fast 
		// way.
		this.queues = new Map();

		// create a map for fast queue access. Means that this is 
		// a shortcut to access the queue for a specific pool. If 
		// a pool is «read, write» it is put into this map twice, once
		// as «read» and once as «write».
		this.queueMap = new Map();


		// storage for the connections
		this.pools = new Map();

		// storage for nodes
		this.nodes = new Set();

		// check for the ttl regularly
		this.checkInterval = setInterval(this.executeTTLCheck.bind(this), this.ttlCheckInterval);
	}




	printStats() {
		log.info(`============= Cluster Stats =============`);
		log.info(`Driver: ${this.driverName}; TTL: ${this.ttl}`);
		log.info(`Queue / Max: ${this.queueLength} / ${this.maxQueueLength}`);
		log.info(`Pool count: ${this.pools.size}`);
		for (const [name, pool] of this.queueMap.entries()) {
			log.info(`- pool ${name} has ${pool.size} idle connections`);
		}

		log.info(`Node count: ${this.nodes.size}`);
		for (const node of this.queueMap.values()) {
			node.printStats();
		}
	}







	/**
	 * loads the vendor specific implmentations which are used
	 * to build the SQL queries, the connections and analyzers
	 */
	loadVendorModules() {

		// load the connection modules
		try {
			this.ConnectionConstructor = require(`related-${this.driverName}-connection`);
		} catch(e) {
			throw new Error(`Failed to load the ${this.driverName} connection driver: ${e}`);
		}

		// load the query builder nodule
		try {
			this.QueryBuilderConstructor = require(`related-${this.driverName}-query-builder`);
		} catch(e) {
			throw new Error(`Failed to load the ${this.driverName} query builder: ${e}`);
		}

		// load the query compiler nodule
		try {
			let QueryCompilerConstructor = require(`related-${this.driverName}-query-compiler`);
			this.compiler = new QueryCompilerConstructor();
		} catch(e) {
			throw new Error(`Failed to load the ${this.driverName} query compiler: ${e}`);
		}

		// load the analyzer nodule
		try {
			this.AnalyzerConstructor = require(`related-${this.driverName}-analyzer`);
		} catch(e) {
			throw new Error(`Failed to load the ${this.driverName} database analyzer: ${e}`);
		}
	}











	/**
	 * check for connection requests that are taking too long
	 */
	executeTTLCheck() {
		for (let queue of this.queues.values()) {
			while (queue.length && queue.getLast().isExpired(this.ttl)) {
				let request = queue.shift();

				// cancel item
				request.abort(new Error(`The connection request for a database connection timed out after ${this.ttl} seconds!`));

				// remove from other queues
				this.removeFromQueue(request);

				// get the user some information about the cluster
				this.printStats();
			}
		}
	}






	/**
	 * queues are used to store queries that cannot be executed 
	 * immediately. each db node has one or mode pool memberships,
	 * a queue is set up for each unique combination of those memberships.
	 * Queries are put in each queue that has its specific pool registered 
	 * on it. this makes it possible that when a free connection is 
	 * available the best matching query can be executed on that connection.
	 *
	 * since queries are potentially stored in multiple queues it will be 
	 * removed from all queues as soon its being executed. using this 
	 * algorithm the pool should always be as balanced as possible.
	 *
	 * @param {node} node the db node
	 */
	setUpQueue(node) {

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
	setUpPool(node) {

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



		// clean up the pools if the node goes down
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
	addNode(configuration) {
		if (this.ended) return Promise.reject(new Error('The cluster has ended, cannot add a new node!'));
		else {
			return new Promise((resolve, reject) => {
				// create node instance, set some sane defaults 
				// (this should work for most CIs)
				let node = new Node({
					  host 					: configuration.host || 'localhost'
					, username  			: configuration.username || configuration.user
					, password  			: configuration.password || configuration.pass || ''
					, port 					: configuration.port
					, maxConnections 		: configuration.maxConnections || configuration.max || 100
					, pools 				: (configuration.pools || (configuration.pool ? [configuration.pool] : ['read', 'write'])).sort()
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
	handleIdleConnection(node, connection) {
		log.debug('Got idle DB connection for pools «'+node.compositeName+'»');

		// checl if we got a connection request wiating for us
		if (this.queues.has(node.compositeName) && this.queues.get(node.compositeName).length) {
			log.debug(`Got queries waiting for a connection in the queue, executing it`);

			// we got a connection request to execute
			let request = this.queues.get(node.compositeName).shift();

			// lets now check the other compatible queues for 
			// the same connection request, remove it there
			this.removeFromQueue(request);

			// return to the caller
			request.execute(connection);
		}
		else {
			log.debug(`No queries waiting for a connection in the queue, adding to pool`);

			// add the connection to the pools
			for (let poolName of node.pools) this.pools.get(poolName).push(connection.id, connection);
		}
	}





	removeFromQueue(request) {
		let queues = this.queueMap.get(request.pool);					
		for (let q of queues) {
			if (q.has(request.id)) q.remove(request.id);
		}
	}




	addtoQueue(poolName, request) {

		// add to all relevant queues
		for (let queue of this.queueMap.get(poolName)) {
			queue.push(request.id, request);
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
	getDBConnection(poolName) {
		log.debug(`Got a connection request for the pool «${poolName}»`);

		if (this.ended) return Promise.reject(new Error('The cluster has ended, cannot get connection!'));
		else if (this.pools.has(poolName) && this.pools.get(poolName).length) {
			let connection = this.pools.get(poolName).shift();

			connection.pools.forEach((pn) => {
				if (pn !== poolName) this.pools.get(pn).remove(connection.id);
			});

			log.debug(`Got a connection from the pool «${poolName}», returning to user`);

			return Promise.resolve(connection);
		}
		else if (!this.queueMap.has(poolName) || !this.queueMap.get(poolName).size) {
			log.debug(`The cluster doesnt serve the requested pool «${poolName}»!`);

			return Promise.reject(new Error('Cannot get connection, no host is serving the pool «'+poolName+'»!'));
		} else if (this.queueLength >= this.maxQueueLength) {
			log.debug(`The cluster queue is full, cannot get connection!`);

			return Promise.reject(new Error('The connection queue is overflowing, request rejected!'));
		} else {
			log.debug(`No connection available in the pool «${poolName}», adding to queue`);

			return new Promise((resolve, reject) => {

				// create a new connection request
				let request = new ConnectionRequest(poolName, resolve, reject);

				this.addtoQueue(poolName, request);
			});
		}
	}









	/**
	 * returns a connection form the given pool, does 
	 * remove it from the pool itself, it cannot be reused
	 * and must be ended after it was used
	 *
	 * @param {string} poolName the name of the pool
	 * 				   the connection must be from
	 *
	 * @returns {Promise} 
	 */
	getConnection(poolName) {
		return this.getDBConnection(poolName).then((connection) => {
			log.debug(`Got a connection from the pool «${poolName}», removing from the pool, returning to user`);

			connection.removeFromPool();

			return Promise.resolve(connection);
		});
	}












	/**
	 * executes a query on the next available db connection
	 *
	 * @param {object} query the query definition
	 *
	 * @returns {Promise}
	 */
	query(queryContext) {

		// first lets check the users input
		if (typeof queryContext !== 'object' || queryContext ===  null) return Promise.reject(new Error('Expected a query context object!'));
		else if (typeof queryContext.pool !== 'string') return Promise.reject(new Error('Missing the pool property on the query context!'));
		else {

			log.debug(`Got a query request for the pool «${queryContext.pool}»`);

			// adding new ast support!
			if (!queryContext.isReady() && queryContext.ast) {
				log.debug(`The query is not ready, but has an AST, compiling it`);

				return this.compiler.compile(queryContext).then(() => {
					log.debug(`The AST query is now ready, executing it`);

					queryContext.sql += ';';
					return this.query(queryContext);
				});
			}
			else {
				// the oldschool way to do things
				return this.getDBConnection(queryContext.pool).then((connection) => {
					log.debug(`Got a connection from the pool «${queryContext.pool}», executing query`);

					// nice, we got a connection, let us check if we 
					// may have to render anything
					if (queryContext.isReady()) {
						log.debug(`The query is ready, executing it`);

						return connection.query(queryContext);
					}
					else {
						log.debug(`The query is not ready, rendering it`);
						
						// let the querybuilder create sql
						return new this.QueryBuilderConstructor(connection).render(queryContext).then(() => {
														
							// execute query
							return connection.query(queryContext);
						});
					}
				});
			}
		}
	}









	/**
	 * lets a outside user with a conenction render aquery. 
	 * used for transactions
	 *
	 * @param {object} connection the connection to use for escaping
	 * @param {object} query the query definition
	 *
	 * @returns {Promise} 
	 */
	renderQuery(connection, queryContext) {
		if (queryContext.isReady()) return Promise.resolve();
		else return new this.QueryBuilderConstructor(connection).render(queryContext);
	}










	/**
	 * render an ast based query
	 *
	 * @param {object} query the query definition
	 *
	 * @returns {Promise} 
	 */
	renderASTQuery(queryContext) {
		if (queryContext.isReady()) return Promise.resolve();
		else if (queryContext.ast) {
			return this.compiler.compile(queryContext).then(() => {
				queryContext.sql += ';';
				return Promise.resolve();
			});
		}
		else return Promise.reject(new Error('Cannot render non AST based query!'));
	}









	/**
	 * gets the description for a db
	 *
	 * @param {array|string} databaseNames the name of the db to describe
	 *
	 * @returns {Promise}
	 */
	describe(databaseNames) {
		return this.getConnection('read').then((connection) => {
			return (new this.AnalyzerConstructor(connection)).analyze(databaseNames).then((description) => {

				// end the connection, it should not be used anymore
				connection.end();

				return Promise.resolve(description);
			}).catch((err) => {

				// end the connection, it should not be used anymore
				connection.end();

				return Promise.reject(err);
			});
		});			
	}








	/**
	 * ends the cluster
	 *
	 * @param {boolean} endNow aborts all queued queries
	 *
	 * @returns {Promise}
	 */
	end(endNow) { 
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
						resolve();
					});

				}
			});
		}
	}
};