!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, argv 			= require('ee-argv')
		, Promise 		= (Promise || require('es6-promise').Promise)
		, arg 			= require('ee-arguments');


	var   dev 			= argv.has('dev-db')
		, debug 		= argv.has('debug-db');


	module.exports = new Class({
		  inherits: EventEmitter

		// mode of operation
		, _validModes: ['readonly', 'readwrite', 'writeonly']

		// max connections
		, _maxConnections: 50

		// number of connections beeing created at the moement
		, _creatingCount: 0

		// currently idle connection count
		, _idle: 0

		// prefetch in % (e.g. 10 = 10%, if you hav a max of 50 connections, there shoudl always be 5 idling connections)
		, _prefetchPercent: 10

		// time between two failed connection attempts
		, _trotthleValue: 10


		// max connections on the node
		, maxConnections:{get: function() {
			return this._maxConnections;
		}}

		// number of created connections
		, count:{get: function() {
			return this._connections.length + this._creatingCount;
		}}

		// number of idle connections
		, idle: {get: function() {
			return this._idle;
		}}

		// return the percentage of idle connections
		, idlePercent: {get: function() {
			return Math.round(((this._creatingCount + this._idle)/this.maxConnections)*100);
		}}

		// connections have an unique id, used for debugging
		, __connectionId: 0

		, _connectionId: {get: function() {
			if (this.__connectionId >= 9e15) this.__connectionId = 0;
			return ++this.__connectionId;
		}}

		// number of ms we must wait until we can attempt to create a new connection
		, _throttleTimeout: null


		/**
		 * class constructor
		 *
		 * @param <Object> options
		 * @param <Object> driver class
		 * @param <String> node mode -> readonly, readwrite, writeonly
		 */
		, init: function(options, driver, mode) {
			if (this._validModes.indexOf(mode) === -1) throw new Error('The mode «'+mode+'» is not supported!').setName('InvalidModeException');

			// connections pool
			this._connections = [];

			// storage that contains all connections that are open
			this._connectionStore = [];

			// node mode
			this.mode 		= mode;

			// driver
			this.driver 	= driver;
			this.options 	= options;

			if (options.maxConnections) this._maxConnections = options.maxConnections;

			// fill the pool
			this._createConnection();
		}




		/**
		 * end all connections on the node
		 */
		, end: function() {
			this._ended = true;

			return new Promise(function(resolve, reject) {
				var creating = this._creatingCount;

				if (creating) {

					// close conenctions that are beeing created now
					this.on('connection', function(connection) {

						// close
						connection.end().then(function() {
							if (--creating === 0) resolve();
						});
					});
				}
				else resolve();
			}.bind(this)).then(function() { log.warn(this._connectionStore.length);
				return Promise.all(this._connectionStore.map(function(connection) {
					if (connection._ended) return Promise.resolve();
					else return connection.end();
				}.bind(this)));
			}.bind(this));
		}


		/**
		 * the _setIdle method increases the idle counter
		 */
		, _setIdle: function() {
			if (dev) log.info('setIdle was called. idlePercent: '+this.idlePercent+'% ...');
			this._idle++;
		}

		/**
		 * the _setIdle method decreases the idle counter and asks for more connections
		 */
		, _setBusy: function() {
			if (dev) log.info('setBusy was called. idlePercent: '+this.idlePercent+'% ...');
			this._idle--;
			process.nextTick(this._createConnection.bind(this));
		}


		/**
		 * the _createConnection method checks if new connections must be made, it manages also the
		 * throttling in case of an error
		 */
		, _createConnection: function() {
			var connection;

			if(dev) log('creating connection ...');

			// no throtling?
			if (this._throttleTimeout === null){
				if(dev) log('count: '+this.count+', idle: '+this.idle+', creating: '+this._creatingCount+', maxConnections: '+this.maxConnections+', idlePercent: '+this.idlePercent+'%, prefetch: '+this._prefetchPercent+'%');

				// dont make too many connections
				if (this.count < this.maxConnections && this.idlePercent < this._prefetchPercent) {
					this._executeCreateConnection();
				}
			}
			else {
				if(dev) log('throtthling connection creation «'+this._throttleTimeout+'» msec ...');
				// throtle connection request, dont attack the server when errors occur
				setTimeout(function() {
					// dont make too many connection
					if (this.count < this.maxConnections && this.idlePercent < this._prefetchPercent) {
						this._executeCreateConnection();
					}
				}.bind(this), this._throttleTimeout);
			}
		}


		/**
		 * the _executeCreateConnection method actually makes a new connection
		 */
		, _executeCreateConnection: function() {
			var connection;

			// dont make new connections after the node has ended
			if (this._ended) return;

			connection = new this.driver(this.options, this._connectionId);


			this._creatingCount++;

			connection.on('load', function(err) {
				this._creatingCount--;

				if (err) {
					log.warn('Failed to establish a connection on «'+this.options.host+':'+this.options.port+'»: '+err);
					this._throttle();
					this._createConnection();
				}
				else {
					if(dev) log('connection created ...');
					this._throttleTimeout = null;
					this._connections.push(connection);
					this._connectionStore.push(connection);

					connection.once('end', function(){
						var index, storeIndex;

						index = this._connections.indexOf(connection);
						if (index >= 0) this._connections.splice(index, 1);

						storeIndex = this._connectionStore.indexOf(connection);
						if (storeIndex >= 0) this._connectionStore.splice(storeIndex, 1);

						if(dev) log('connection has ended, removed connection at index «'+index+'»...');

						// is there need to create new connections?
						this._createConnection();
					}.bind(this));

					connection.on('busy', this._setBusy.bind(this));
					connection.on('idle', this._setIdle.bind(this));

					this._setIdle();

					this.emit('connection', connection);
				}
			}.bind(this));

			// make more connections, the _createConnection method will abort
			// this if limits / requirements are reached
			process.nextTick(this._createConnection.bind(this));
		}


		/**
		 * the _throttle method increases the thottle interval
		 */
		, _throttle: function(){
			this._throttleTimeout = this._throttleTimeout === null ? 10 : this._throttleTimeout * 1.5;
			if (this._throttleTimeout > 30000) this._throttleTimeout = 30000;
		}
	});
}();
