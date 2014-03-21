
	
	var   Class 			= require('ee-class')
		, log 				= require('ee-log');



	var Cluster = require('./')




	var or = function(){
		var a = new Array(Array.prototype.slice.call(arguments));
		a.mode = 'or';
		return a;
	}

	var and = function(){
		var a = new Array(Array.prototype.slice.call(arguments));
		a.mode = 'and';
		return a;
	}



	var cluster = new Cluster({type:'mysql'});

	cluster.addNode('readwrite', {
		  username: 	'eb-client'
		, password: 	''
		, host: 		'10.0.100.1'
	});


	cluster.getConnection(function(err, connection){
		connection.query( 'query', [
			{
				filter: {
					event: {
						_: [
						 	  { id: 5 }
							, { deleted: null }
							, [
								  { id: 66 }
								, { deleted: function(){ return {fn: 'notNull'} } }
								, { 
									  id: 66 
									, name: 'hui'
								}
							]
						]
						, created: new Date()
						, updated: new Date()
						, tenant:{
							id:55
						}
						, group: [
							{ id: 3 }
							, {name: 'fabi'}
						]
						, random : function(){
							return {
								  operator: 'lte'
								, query:{
									filter: {
										fabian: {
											  sex: true
											, age: 45
										}
									}
									, from: 'vandi'
									, database: 'eventbox'
									, select: [
										'id'
									]
									, limit: 1
								}
							};
						}
					}
				}
				, from: 'event'
				, database: 'eventbox'
				, select: ['id', { 
					  alias: 'bestORMEver' 
					, query: {
						filter: {
							fabian: {
								  sex: true
								, age: 45
							}
						}
						, from: 'vandi'
						, database: 'eventbox'
						, select: [
							'id'
						]
						, limit: 1
					}
				}, function(){return {fn: 'max', property: 'deleted'}}]
				, group: ['deleted']
			}
		], function(err, result){
			log(err, result);
		});
	});

