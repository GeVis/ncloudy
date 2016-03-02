var ZooKeeper = require('zk');
var utils = require('utility');
var async = require('async');

function ZKClient(options, onCreate, onDelete, onUpdated) {
    options.host = options.host ? options.host : 'localhost';
    options.port = options.port ? options.port : '2181';
    options.path = options.path ? options.path : '/getu/test';
    this.options = options;
    this.client = new ZooKeeper({
        connect: options.host + ':' + options.port,
        timeout: 200000,
        debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false,
        retries: Number.MAX_VALUE
    });
    this.onCreate = onCreate;
    this.onDelete = onDelete;
    this.onUpdated = onUpdated;
    this.nodes = {};

    var zk = this;
    zk.client.connect().then(function() {
        zk.client.exists(zk.options.path).then(function(reply) {
            async.series([function(cbk) {
                if (reply.stat) {
                    cbk(null);
                } else {
                    zk.client.zk.mkdirp(zk.options.path, function(err) {
                        cbk(err);
                    });
                }
            }, function(cbk) {
                zk.getAll(onUpdated);
                cbk(null);
            }]);
        }).catch(function(err) {
            return console.error(err);
        });
    });


};

ZKClient.prototype.add = function(host, port, weight) {
    weight = weight || 1;
    var url = host + ':' + port;
    var name = utils.md5(url);
    this.client.create(this.options.path + '/' + name, new Buffer([host, port, weight].join('|'))).then(function() {

    }).catch(function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.delete = function(host, port) {
    var url = host + ':' + port;
    var name = utils.md5(url);
    this.client.delete(this.options.path + '/' + name).catch(function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.getAll = function(cb) {
    var zk = this;
    zk.client.getChildren(zk.options.path, true).then(function(reply) {
        reply.watch.then(function(event) {
            if (event.type == 'child') {
                zk.getAll(zk.onUpdated);
            }
        });
        var children = reply.children;
        if (children && children.length > 0) {
            async.map(children, function(child, cbk) {
                if (child in zk.nodes) {
                    cbk(null, zk.nodes[child]);
                } else {
                    zk.get(child, cbk);
                }
            }, cb);
        } else {
            cb('No nodes available!', []);
        }
    });
}

ZKClient.prototype.get = function(name, cb) {
    var path = this.options.path + '/' + name;
    var zk = this;
    this.client.get(path, true).then(function(reply) {
        reply.watch.then(function(event) {
            if (event.type == 'deleted') {
                zk.onDelete(zk.nodes[name]);
                delete zk.nodes[name];
            }
        });
        var arr = reply.data.toString().split('|');
        if (arr.length < 3) {
            return cb('Data error: ' + path);
        }
        cb(null, {
            host: arr[0],
            port: parseInt(arr[1]),
            weight: parseFloat(arr[2]),
            hash: name
        });
    }, function(err, data, stats) {
        if (err || !data || data.length === 0) {
            console.error(path, err);
            return cb(err);
        }
    })
}

module.exports = ZKClient;
