var ZKClient = require('./zk');
var async = require('async');

function Cloudy(options, onCreate, onReady) {
	this.onCreate = onCreate;
	this.onReady = onReady;
	this.ready = false;
	var cloudy = this;
	var cbk = function(err, nodes) {
		if (err) {
			return console.error(err);
		}
		async.map(nodes.filter(function(node) {
			return node && node.host;
		}), function(node, cbk) {
			cbk(null, onCreate(node));
		}, function(err, clients) {
			cloudy.clients = clients.filter(function(client) {
				return client;
			});
			cloudy.size = clients.length;
			if (!cloudy.ready) {
				cloudy.ready = true;
				cloudy.onReady();
			}
		});
	};
	this.zk = new ZKClient(options, cbk, cbk);
}

Cloudy.prototype.register = function(host, port, weight) {
	weight = weight || 1;
	this.zk.add(host, port, weight);
}

Cloudy.prototype.unregister = function(host, port) {
	this.zk.delete(host, port);
}

Cloudy.prototype.client = function() {
	if (!this.size) {
		return null;
	}
	var idx = Math.floor(Math.random() * this.size);
	return this.clients[idx];
}

module.exports = Cloudy;