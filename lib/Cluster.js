!function(){

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

		// drivers aka db specific connectors
		, drivers: {
			mysql: '../../ee-mysql-connection'
		}

		// the laoded driver
		, driver: null


		// loaded cluster nodes
		, nodes: {
			  readonly: 	[]
			, readwrite: 	[]
			, writeonly: 	[]
		}

		// connection pool
		, connections: {
			  readonly: 	[]
			, readwrite: 	[]
			, writeonly: 	[]
		}

		// request queue
		, queue: {}


		/**
		 * class constructor
		 *
		 * @param <Object> options
		 */
		, init: function(options){
			this.type = options.type;
			this.allowReadsOnWritabelNodes = !!options.allowReadsOnWritabelNodes;

			this._loadDriver();
			this._createQueue('readonly');
			this._createQueue('readwrite');
		}



		/**
		 * the _loadDriver method loads the db driver
		 */
		, _loadDriver: function(){
			var type = this.type;

			// valid node type?
			if (this.drivers[type]){

				// load driver
				try {
					this.driver = require(this.drivers[type]);
					if(dev) log('loaded db driver «'+this.drivers[type]+'» ...');
				} catch(e) {
					log(e);
					throw new Error('Failed to load the «'+type+'» module, please make sure the «'+this.drivers[type]+'» is installed properly!').setName('DriverException');
				}
			} 
			else throw new Error('Unsupported db type «'+type+'»!').setName('InvalidTypeException');
		}



		/**
		 * the _createQueue initilaizes the queue system
		 */
		, _createQueue: function(id){
			// create connection queues
			this.queue[id] = new Queue({
				  ttl: 10000
				, max: 100000
			});

			this.queue[id].on('timeout', function(callback){
				callback(new Error('Failed to get a connection!').setName('TimeoutException'));
			}.bind(this));

			this.queue[id].on('error', function(){
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
		, addNode: function(){
			var   options 	= arg(arguments, 'object', {})
				, mode 		= arg(arguments, 'string', 'readonly')
				, node;


			// create node instance
			node = new Node(options, this.driver, mode);

			// remove ended nodes
			node.on('end', function(){
				var index = this.nodes[mode].indexOf(node);
				if (index >= 0) this.nodes[mode].splice(index, 1);
				if(dev) log('removed «'+mode+'» node ...');
			}.bind(this));

			// emit connection
			node.on('connection', function(connection){
				if(dev) log('got free «'+mode+'» connection ...');

				this.connections[mode].push(connection);

				// remove conenction from pool when closed
				connection.on('end', function(){
					var index = this.connections[mode].indexOf(connection);
					if (index >= 0) this.connections[mode].splice(index, 1);

					if(dev) log('«'+mode+'» connection has ended...');
				}.bind(this));

				// add idling connections to pool of available connections
				connection.on('idle', function(){
					if(dev) log('got idling «'+mode+'» connection ...');
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
		, _execute: function(){

			// readonly stuff
			if (this.queue.readonly.length){
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
			if (this.queue.readwrite.length){
				if (this.connections.writeonly.length) {
					this.queue.writeonly.get()(null, this.connections.writeonly.shift());
					if(dev) log('returned «writeonly» connection for a writing query ...');
				}
				else if (this.connections.readwrite.length){
					this.queue.readwrite.get()(null, this.connections.readwrite.shift());
					if(dev) log('returned «readwrite» connection for a writing query ...');
				}
			}
		}


		/**
		 * the query method executes a query on the next availabel connection
		 *
		 * @param <Object> Query
		 * @param <Function> callback
		 */
		, query: function(query, callback){
			var args = arguments;

			this.getConnection(function(err, connection){
				if (err) arg(args, 'function')(err);
				else connection.query.apply(connection, Array.prototype.slice.call(args));
			}.bind(this));
		}



		/**
		 * the getConnection method returns a connection when one becomes available.
		 * parameters may be passed in any order
		 *
		 * @param <Function> callback
		 * @param <Boolean> readonly connection, optional, defaults to true
		 */
		, getConnection: function(){
			var   callback 	= arg(arguments, 'function')
				, readonly 	= arg(arguments, 'boolean', true)
				, mode 		= readonly ? 'readonly' : 'readwrite';

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
		, describe: function(callback){
			this.getConnection(function(err, connection){
				if (err) callback(err);
				else connection.describe(callback);
			}.bind(this));
		}



		/**
		 * the getConnection method returns a connection when one becomes available
		 *
		 * @param <Function> callback
		 */
		, createTransaction: function(callback){
			this.getConnection(false, function(err, connection){
				if (err) callback(err);
				else {
					connection.startTransaction();
					callback(null, connection);
				}
			}.bind(this));
		}
	});
}();
