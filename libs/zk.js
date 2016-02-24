var ZooKeeper = require('node-zookeeper-client');
var utils = require('utility');
var async = require('async');

function ZKClient(options, onInit, onUpdate) {
    options.host = options.host ? options.host : 'localhost';
    options.port = options.port ? options.port : '2181';
    options.path = options.path ? options.path : '/getu/test';
    this.options = options;
    this.client = ZooKeeper.createClient(options.host + ':' + options.port, {
        timeout: 200000,
        debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
        host_order_deterministic: false,
        retries: Number.MAX_VALUE
    });
    this.onInit = onInit;
    this.onUpdate = onUpdate;

    var zk = this;
    zk.client.connect();
    zk.client.exists(zk.options.path, function(err, stat) {
        if (err) {
            return console.error(err);
        }
        async.series([function(cbk) {
            if (stat) {
                cbk(null);
            } else {
                zk.client.mkdirp(zk.options.path, function(err, path) {
                    cbk(err);
                });
            }
        }, function(cbk) {
            zk.getAll(zk.onInit);
            cbk(null);
        }]);
    })

};

ZKClient.prototype.add = function(host, port, weight) {
    weight = weight || 1;
    var url = host + ':' + port;
    var name = utils.md5(url);
    this.client.create(this.options.path + '/' + name, new Buffer([host, port, weight].join('|')), function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.delete = function(host, port) {
    var url = host + ':' + port;
    var name = utils.md5(url);
    this.client.remove(this.options.path + '/' + name, function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.getAll = function(cb) {
    var zk = this;
    zk.client.getChildren(zk.options.path, function(event) {
        if (event.getType() == ZooKeeper.Event.NODE_CHILDREN_CHANGED) {
            zk.getAll(zk.onUpdate);
        }
    }, function(err, children, stats) {
        if (err) {
            return cb(err);
        }
        if (children && children.length > 0) {
            async.map(children, function(child, cbk) {
                zk.get(child, cbk);
            }, cb);
        } else {
            cb(err, []);
        }
    });
}

ZKClient.prototype.get = function(name, cb) {
    var path = this.options.path + '/' + name;
    this.client.getData(path, function(err, data, stats) {
        if (err || !data || data.length === 0) {
            console.error(path, err);
            return cb(err);
        }
        var arr = data.toString().split('|');
        if (arr.length < 3) {
            return cb('Data error: ' + path);
        }
        cb(null, {
            host: arr[0],
            port: parseInt(arr[1]),
            weight: parseFloat(arr[2]),
            hash: name
        });
    })
}

module.exports = ZKClient;
