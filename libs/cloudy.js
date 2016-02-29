var ZKClient = require('./zk');
var async = require('async');

function Cloudy(options, onCreate, onDelete, onReady) {
	this.onCreate = onCreate;
	this.onReady = onReady;
	this.onDelete = onDelete;
	this.ready = false;
	this.cur = 0;
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
			if (cloudy.clients && cloudy.clients.length) {
				clients.forEach(cloudy.onDelete);
			}
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
	this.cur = (this.cur + 1) % this.size;
	return this.clients[this.cur];
}

module.exports = Cloudy;