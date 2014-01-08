
	
	var   Class 			= require('ee-class')
		, log 				= require('ee-log')
		, assert 			= require('assert')
		, travis 			= require('ee-travis');



	var Cluster = require('../')





	describe('The Cluster', function(){
		var cluster;

		it('should not throw when instantiated', function(){
			cluster = new Cluster({type:'mysql'});
		});

		it('should accept new nodes', function(){
			cluster.addNode('readwrite', {
				  username: 	'eb-client'
				, password: 	''
				, host: 		'10.0.100.1'
			});
		});

		it('should be able to return an open conenction', function(done) {
			cluster.getConnection(done);
		});
	});
	