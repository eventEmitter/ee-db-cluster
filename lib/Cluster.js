!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments')
		, Queue 		= require('ee-ttl-queue');



	module.exports = new Class({
		inherits: EventEmitter

		// drivers aka db specific connectors
		, drivers: {
			mysql: 'ee-mysql-connection'
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
				} catch(e) {
					throw new Error('Failed to load the «'+type+'» module, pease make sure the «'+this.drivers[type]+'» is installed properly!').setName('DriverException');
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
				  ttl: 2000
				, max: 100000
			});

			this.queue[id].on('timeout', function(callback){
				callback(new Error('Failed to get a connection in time, aborting!').setName('TimeoutException'));
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
		, addNode: function(options, mode){
			var node;

			mode = mode || 'readonly';

			// create node instance
			node = new Node(options, this.driver, mode);

			// remove ended nodes
			node.on('end', function(){
				var index = this.nodes[mode].indexOf(node);
				if (index >= 0) this.nodes[mode].splice(index, 1);
			}.bind(this));

			// remove ended nodes
			node.on('connection', function(connection){
				this.connections[mode].push(connection);

				// remove conenction from pool when closed
				connection.on('end', function(){
					var index = this.connections[mode].indexOf(connection);
					if (index >= 0) this.connections[mode].splice(index, 1);
				}.bind(this));

				// add idling connections to pool of available connections
				connection.on('idle', function(){
					this.connections[mode].push(connection);
				}.bind(this));
			}.bind(this));

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
				}
				else (this.connections.readwrite.length){
					this.queue.readwrite.get()(null, this.connections.readonly.shift());
				}
			}

			// writing stuff
			if (this.queue.readwrite.length){
				if (this.connections.writeonly.length) {
					this.queue.writeonly.get()(null, this.connections.readwrite.shift());
				}
				else (this.connections.readwrite.length){
					this.queue.readwrite.get()(null, this.connections.readwrite.shift());
				}
			}
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
			
			// trigger queue
			this._execute();
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
