(function() {
	'use strict';


	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, LinkedList 	= require('linkd')
		, argv 			= require('ee-argv')
		;





	/**
	 * a node represents one databse host. It creates connections and
	 * checks if its up when queries start to fail. the outside cannot
	 * request connections from it, it creates them itself as they are
	 * needed.
	 * a node with a lower max connections value is automatically used 
	 * less than a host with more connections. this isn't enforced by 
	 * any algorithm, it happens naturally because the node sends
	 * less connections to the pool and has thus a lower chance to get
	 * selected for a query.
	 *
	 */



	module.exports = new Class({
		inherits: EventEmitter


		// max connections
		, maxConnections: 50

		// number of connections beeing created at the moement
		, creatingCount: 0

		// currently idle connection count
		, idleCount: 0

		// prefetch in % (e.g. 10 = 10%, if you hav a max of 50 connections, there shoudl always be 5 idling connections)
		, prefetchPercent: 10

		// time in ms between two failed connection attempts
		, throttleTime: 10

		// indicates if we're currently throttling the creation
		// of new connections
		, throttling: true

		// flags if the host was ended, if yes, we should not
		// create any new connections anymore
		, ended: false




		// flags if error checking is already in progress
		, errorChecking: false

		// the timestamp of the last error check, we should not check
		// too often since a long running query can trigger such checks
		, lastErrorCheck: Date.now()

		// how many ms to wait in between error checks
		, errorCheckInterval: 30000

		// how many ms to wait for a timeout
		, errorCheckTimeout: 30000



		// a unique identifier
		, id: null

		// pool composite name
		, compositeName: null




		// connection id counter
		, connectionIdValue: 0




		// returns the percentage of idle connections, 
		// connections currently created are also counted
		// as idle connections
		, idle: {
			get: function() {
				return Math.round((this.idleCount+this.creatingCount)/this.maxConnections*100);
			}
		}


		// the count is the number of open connections on this node
		// including all connections that are currently beeingmade
		, count: {
			get: function() {
				return this.connections.length;
			}
		}






		/**
		 * class constructor
		 *
		 * @param <Object> options
		 * @param <Object> ConnectionConstructor diver class
		 * @param <String> node mode -> readonly, readwrite, writeonly
		 */
		, init: function(config) {

			// holds the connections that are currently open on 
			// this host, also used for connectivity checks (the
			// oldest connections are the topmost items in the list)
			this.connections = new LinkedList();


			// the host must be part of one or more pools
			this.pools = config.pools;



			// driver
			this.ConnectionConstructor = config.ConnectionConstructor;


			// credentials and timeouts
			this.config = config;


			// the user can set a custom connection limit
			if (config.maxConnections) this.maxConnections = config.maxConnections;




			// we need a truly unique id
			this.id = Symbol('nodeId');

			// set our composite name
			this.compositeName = config.pools.join('/');



			// fill the pool
			this.createConnection();


			// tell the outside if we have succeeded
			this.once('connection', () => {
				process.nextTick(() => {
					this.emit('load');
				});
			})
		}


		




		/**
		 * checks if its possible to create a new connection
		 * under all given constraints. if everything looks ok
		 * the executeCreateConnection method is called which
		 * create the actual connection
		 */
		, createConnection: function() { //log(this.ended, this.idle, this.prefetchPercent, this.count, this.maxConnections);

			// first we need to check our status and if we're 
			// allowed to create more connections
			if (!this.ended && this.idle < this.prefetchPercent && this.count < this.maxConnections) {

				// if there were connection errors the pace on which
				// we're creating new connections is reduced with each 
				// attempt to create a new connection
				if (this.throttling) {

					// in throttling mode there can not be more 
					// than one connection attempt the any given time
					if (this.creatingCount === 0) {

						// increase the throttling time on each run by 10%
						this.throttleTime = Math.ceil(this.throttleTime*1.1);

						// wait until the next attempt to connect
						setTimeout(() => {

							// wait for the result, decide what to do
							// after that
							this.executeCreateConnection().then(() => {

								// nice, we were successful, lets reset all the 
								// throttling stuff
								this.throttling = false;
								this.throttleTime = 10;

								// we're ready to create as many connections as 
								// needed
								this.createConnection();
							}).catch((err) => {

								// try again
								this.createConnection();
							});
						}, this.throttleTime);
					}
				}
				else {

					// create the connection now
					this.executeCreateConnection().then(() => {});

					// create as many connections as needed, create one,
					// call this method again
					process.nextTick(this.createConnection.bind(this));
				}
			}
		}








		/**
		 * creates a new connection
		 */
		, executeCreateConnection: function() {
			let connection = new this.ConnectionConstructor(this.config, this.getConnectionId(), this);

			// store connection
			this.connections.push(connection.id, connection);


			// increase create conenction indicator
			this.creatingCount++;


			// connect
			return connection.connect().then(() => {

				// decrease it, so that new connecitons can be made
				this.creatingCount--;

				// make sure the connection is removed as soon as it ends
				connection.once('end', (err) => {
					if (this.connections.has(connection.id)) this.connections.remove(connection.id);

					// get a new conenction
					this.createConnection();
				});

				// not throttling anymore
				this.throttling = false;

				// the connection my report connectivity problems 
				connection.on('connectivityProblem', this.handleConnectivityProblem.bind(this));

				// tell the cluster about the connection
				this.emit('connection', connection);
			}).catch((err) => {
				log.warn('Failed to create db connection: '+err);

				// decrease it, so that new connecitons can be made
				this.creatingCount--;


				// remove from storage
				if (this.connections.has(connection.id)) this.connections.remove(connection.id);

				// we shoud start throttling connection creation
				this.throttling = true;

				// so, thats a problem, we got a specialized handler
				// for this case, it tries to detect if the host is 
				// available at all
				this.handleConnectivityProblem();


				// throw the error so it bubbles up
				throw err;
			});			
		}









		/**
		 * so, there was a problem with a connection? lets find
		 * out if the host is down or only one connection had its
		 * difficulties
		 *
		 */
		, handleConnectivityProblem: function() {

			// check if we have to check the nodes connectivity at all
			if (!this.ended && !this.errorChecking && (Date.now()-this.errorCheckInterval) > this.lastErrorCheck) {

				// so, were trying to execute a very simple query on 
				// the oldest of all connections, if its dead we're going
				// to kill of all connections so they can be re-established
				// as soon the host is available again

				// update status
				this.errorChecking = true;
				this.lastErrorCheck = Date.now();


				// if there arent any connections, trigger the
				// creation on one, all done with that
				if (!this.connections.length) this.createConnection();
				else {
					let hasTimeout = false;

					// get the oldes connection
					let connection = this.connections.getLast();



					// external timeout check
					let checkTimout = setTimeout(() => {
						this.errorChecking = false;

						// flag as timeout
						hasTimeout = true;

						// the query failed, nuke the host, close all connections
						// then try to create a new one
						this.resetNode();
					}, this.errorCheckTimeout);




					// execute a simple query
					connection.query({
						  SQL: 'SELECT 1;'
						, mode: 'query'
					}).then(() => {

						// nice, we're ok
						this.errorChecking = false;

						// disable the timeout
						if (!hasTimeout) clearTimeout(checkTimout);

					}).catch((err) => {
						this.errorChecking = false;

						// if had a timeout we dont need to do anything here
						if (!hasTimeout) {
							clearTimeout(checkTimout);

							// the query failed, nuke the host, close all connections
							// then try to create a new one
							this.resetNode();
						}
					});
				}
			}
		}






		/**
		 * closes all connections, tries to establish new ones
		 * this is useful if a host goes down and a filover must
		 * be done
		 */
		, resetNode: function() {
			log.warn('The node «'+this.config.username+'@'+this.config.host+':'+this.config.port+'/'+this.config.database+'» is shutting down, ending all idle connections, re-initilizing the pool for this host!');

			// close all connections as soon as possible
			// this will immediatelly end all idle connections
			// all connections executing a query after that query
			// has finished and all idle queries not part of the pool
			// that are not transactions
			for (let connection of this.connections) connection.kill();


			// set up a new linked list
			this.connections = new LinkedList();

			// reset counters
			this.creatingCount = 0;
			this.idleCount = 0;

			this.throttling = true;
			this.throttleTime = 10;


			// request a new connection
			process.nextTick(this.createConnection.bind(this));
		}






		/**
		 * ends all connections on the host
		 */
		, end: function() {

			this.ended = true;


			// close all connections as soon as possible
			// this will immediatelly end all idle connections
			// all connections executing a query after that query
			// has finished and all idle queries not part of the pool
			// that are not transactions
			for (let connection of this.connections) connection.kill();



			// de-reference
			this.connections = null;



			// tell the outside that we're finished
			this.emit('end');
		}




		/**
		 * creates a unique connection id
		 */
		, getConnectionId: function() {
			return (this.connectionIdValue++)+'';
		}
	});
})();
