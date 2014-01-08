!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, argv 			= require('ee-argv')
		, arg 			= require('ee-arguments');


	var   dev 			= argv.has('dev-db')
		, debug 		= argv.has('debug-db');


	module.exports = new Class({
		inherits: EventEmitter

		// mode of operation
		, _validModes: ['readonly', 'readwrite', 'writeonly']

		// all connections
		, _connections: []

		// max connections
		, _maxConnections: 50

		// number of connections beeing created at the moement
		, _creatingCount: 0

		// currently idle connection count
		, _idle: 0

		// prefetch in % (e.g. 10 = 10%, if you hav a max of 50 connections, there shoudl always be 5 idling connections)
		, _prefetch: 10

		// time between two failed connection attempts
		, _trotthleValue: 1000


		// max connections
		, get maxConnections() {
			return this._maxConnections;
		}

		// number of created conenctions
		, get count() {
			return this._connections.length + this._creatingCount;
		}

		// number of idle connections
		, get idle() {
			return this._idle;
		}

		// return the percentage of idle connections
		, get idlePercent() {
			return Math.round(((this._creatingCount + this._idle)/this.maxConnections)*100);
		}


		/**
		 * class constructor
		 *
		 * @param <Object> options
		 * @param <Object> driver class
		 * @param <String> node mode -> readonly, readwrite, writeonly
		 */
		, init: function(options, driver, mode) {
			if (this._validModes.indexOf(mode) === -1) throw new Error('The mode «'+mode+'» is not supported!').setName('InvalidModeException');

			this.mode 		= mode;
			this.driver 	= driver;
			this.options 	= options;

			if (options.maxConnections) this._maxConnections = options.maxConnections;

			// create connection event handler
			this._createConnection();
		}


		/**
		 * the _setIdle method increases the idle counter
		 */
		, _setIdle: function() {
			this._idle++;
		}	

		/**
		 * the _setIdle method decreases the idle cpunter and asks for more conenctions
		 */
		, _setBusy: function() {
			this._idle--;
			setTimeout(this._createConnection.bind(this), 0);
		}


		/**
		 * the _createConnection method checks if new connections must be made, it manages also the
		 * throttling in case of an error
		 */
		, _createConnection: function() {
			var connection;

			if(dev) log('creating conncetion ...');			

			// no throtling (1000 msec)?
			if (this._trotthleValue === 1000){
				if(dev) log('count: '+this.count+', maxConnections: '+this.maxConnections+', idlePercent: '+this.idlePercent+'%, prefetch: '+this._prefetch+'%');

				// dont make too many connections
				if (this.count < this.maxConnections && this.idlePercent < this._prefetch){
					this._executeCreateConnection();
				}
			}
			else {
				if(dev) log('throtthling conncetion creation «'+this._trotthleValue+'» msec ...');
				// throtle connection request, dont attack the server when errors occur
				setTimeout(this._executeCreateConnection.bind(this), this._trotthleValue);
			}
		}


		/**
		 * the _executeCreateConnection method actually makes a new connection
		 */
		, _executeCreateConnection: function(){
			var connection = new this.driver(this.options);

			this._creatingCount++;

			connection.on('load', function(err){
				this._creatingCount--;

				if (err) {
					log.warn('failed to establish a connection on «'+this.options.host+':'+this.options.port+'»: '+err);
					this._throttle();
				}
				else {
					if(dev) log('conncetion created ...');
					this._trotthleValue = 1000;
					this._connections.push(connection);

					connection.on('end', function(){
						var index = this._connections.ref.indexOf(connection);
						if (index >= 0) this._connections.ref.splice(index, 1);
					}.bind(this));
					
					connection.on('busy', this._setBusy.bind(this));
					connection.on('idle', this._setIdle.bind(this));

					this._setIdle();

					this.emit('connection', connection);
				}
			}.bind(this));

			// make more
			setTimeout(this._createConnection.bind(this), 10);
		}


		/**
		 * the _throttle method increases the thottle interval
		 */
		, _throttle: function(){
			this._trotthleValue *= 2;
			if (this._trotthleValue > 30000) this._trotthleValue = 30000; 
		}
	});
}();
