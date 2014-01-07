!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, EventEmitter 	= require('ee-event-emitter')
		, type 			= require('ee-types')
		, arg 			= require('ee-arguments');



	module.exports = new Class({
		inherits: EventEmitter

		// mode of operation
		, _validModes: ['readonly', 'readwrite', 'writeonly']

		// all connections
		, _connections: []

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
			return Math.round(((this._creatingCount + this.idle)/this.maxConnections)*100);
		}

		// number of connections beeing created at the moemebt
		, _creatingCount: 0


		, init: function(options, driver, mode) {
			if (this._validModes.indexOf(mode) === -1) throw new Error('The mode «'+mode+'» is not supported!').setName('InvalidModeException');

			this.mode 		= mode;
			this.driver 	= driver;
			this.options 	= options;

			if (options.maxConnections) this._maxConnections = options.maxConnections;

			// create connection event handler
			this._createConnection();
		}


		, _setIdle: function() {
			this._idle++;
		}	

		, _setBusy: function() {
			this._idle--;
			setTimeout(this._createConnection.bind(this), 0);
		}


		, _createConnection: function() {
			var connection;

			if (this.count < this.maxConnections && this.idlePercent < this.prefetch){
				connection = new this.driver(this.options);

				this._creatingCount++;

				connection.on('load', function(err){
					this._creatingCount--;

					if (err) {
						callback(err);
					}
					else {
						this._connections.push(connection);

						connection.on('end', function(){
							var index = this._connections.ref.indexOf(connection);
							if (index >= 0) this._connections.ref.splice(index, 1);
						}.bind(this));

						connection.on('busy', this.setBusy.bind(this));
						connection.on('idle', this.setIdle.bind(this));

						this.emit('connection');
					}
				}.bind(this));

				// make more
				setTimeout(this._createConnection.bind(this), 0);
			}
		}
	});
}();
