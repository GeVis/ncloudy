var ZooKeeper = require('zookeeper');
var utils = require('utility');
var async = require('async');
var path = require('path');

function ZKClient(options, onCreate, onDelete, onUpdated) {
    options.host = options.host || 'localhost';
    options.port = options.port || '2181';
    options.path = options.path || '/ncloudy';
    options.connect = options.connect || (options.host + ':' + options.port);
    this.options = options;
    this.client = new ZooKeeper({
        connect: options.connect,
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
    new Promise(function(resolve, reject) {
        zk.client.connect(function(err){
            if(err) {
                reject(err);
            } else {
                resolve();
            }
        })
    }).then(function() {
        new Promise(function(resolve, reject){
            zk.client.a_exists(zk.options.path, false, function(rc, error, stat){
                if(rc === -101) {
                    // error: no node, create it;
                    console.log('Node(%s) not exists, mkdirp it.', zk.options.path);
                    zk.client.mkdirp(zk.options.path, function(err){
                        if(err) {
                            reject(err);
                        } else {
                            resolve();
                        }
                    })
                } else if (!rc){
                    resolve()
                } else {
                    var err = new Error(error);
                    err.code = rc;
                    reject(err);
                }
            });
        }).then(function(reply) {
             zk.getAll(onUpdated);
        }).catch(function(err) {
            return console.error(err);
        });
    });


};

ZKClient.prototype.add = function(host, port, weight) {
    var zk = this;
    weight = weight || 1;
    var url = host + ':' + port;
    var name = utils.md5(url);
    new Promise(function(resolve, reject){
        zk.client.a_create(
            zk.options.path + '/' + name, 
            new Buffer([host, port, weight].join('|')),
            ZooKeeper.ZOO_EPHEMERAL, 
            function(rc, error, path){
                if(rc) {
                    var err = new Error(error);
                    err.code = rc;
                    reject(err);
                } else {
                    resolve();
                }
            })
    }).then(function() {

    }).catch(function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.delete = function(host, port) {
    var zk = this;
    var url = host + ':' + port;
    var name = utils.md5(url);
    new Promise(function(resolve, reject){
        zk.client.a_delete_(zk.options.path + '/' + name, 0, function(rc, error){
            if(rc) {
                var err = new Error(error + ',' + zk.options.path + '/' + name);
                err.code = rc;
                reject(err);
            } else {
                resolve();
            }
        })
    }).catch(function(err) {
        if (err) {
            console.error(err);
        }
    });
}

ZKClient.prototype.getAll = function(cb) {
    var zk = this;
    new Promise(function(resolve, reject){
        var watcher = new Promise(function(w_resolve, w_reject){
            zk.client.aw_get_children(zk.options.path, function watch_cb(type, state, path){
                var types = 'child,create,datachanged,deleted,none'.split(',')
                var event = {
                    type: types[type],
                    state: state,
                    path: path
                };
                w_resolve(event);
            },  function child_cb(rc, error, children){
                if(rc) {
                    var err = new Error(error);
                    err.code = rc;
                    reject(err);
                } else {
                    resolve({
                        watch: watcher,
                        children: children
                    });
                }
            })
        });
        
        
    }).then(function(reply) {
        reply.watch.then(function(event) {
            zk.getAll(cb)
        });
        var children = reply.children;
        if (children && children.length > 0) {
            async.map(children, function(child, cbk) {
                if (child in zk.nodes) {
                    cbk(null, zk.nodes[child]);
                } else {
                    zk.get(child, cbk);
                }
            }, function(err, clients){
                cb(err, clients);
            });
        } else {
            cb('No nodes available!', []);
        }
    });
}

ZKClient.prototype.get = function(name, cb) {
    var path = this.options.path + '/' + name;
    var zk = this;
    new Promise(function(resolve, reject){
        var watcher = new Promise(function(w_resolve, w_reject){
            zk.client.aw_get(path, function watch_cb(type, state, path){
                var types = 'none,created,deleted,datachanged,childchanged'.split(',');
                var event = {
                    type: types[type],
                    state: state,
                    path: path
                };
                w_resolve(event);

            },  function data_cb(rc, error, stat, data){
                if(rc) {
                    var err = new Error(error);
                    err.code = rc;
                    reject(err);
                } else {
                    resolve({
                        watch: watcher,
                        data: data
                    });
                }
            })
        })
        
       
    }).then(function(reply) {
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
        var node = {
            host: arr[0],
            port: parseInt(arr[1]),
            weight: parseFloat(arr[2]),
            hash: name
        };      
        zk.nodes[name] = zk.onCreate(node);
        cb(null, zk.nodes[name]);
    }, function(err, data, stats) {
        if (err || !data || data.length === 0) {
            return cb(err);
        }
    })
}

module.exports = ZKClient;
