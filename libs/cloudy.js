var ZKClient = require('./zkClient');
var async = require('async');

function Cloudy(options, onCreate, onDelete, onReady) {
    this.onCreate = onCreate;
    this.onReady = onReady;
    this.onDelete = onDelete;
    this.ready = false;
    this.cur = 0;
    var cloudy = this;
    this.zk = new ZKClient(options, onCreate, function(client) {
        var idx = cloudy.clients.indexOf(client);
        if (idx > -1) {
            cloudy.clients.splice(idx, 1);
        }
        cloudy.onDelete(client);
    }, function(err, clients) {
        cloudy.clients = clients;
        cloudy.size = cloudy.clients.length;
        if (!cloudy.ready) {
            cloudy.ready = true;
            cloudy.onReady();
        }
    });
}

Cloudy.prototype.register = function(host, port, weight) {
    var cloudy = this;
    weight = weight || 1;
    this.zk.add(host, port, weight);
    function exitHandler(options, err) {
        if (options.cleanup) {
            console.log('Unregister Service Server From Cloudy');
            cloudy.unregister(host, port);
        }
        if (err) {
            console.log('Process Error:', err.stack);
        }
        if (options.exit) {
            process.exit();
        }
    }
    //do something when app is closing
    process.on('exit', exitHandler.bind(null,{cleanup:true}));
    //catches ctrl+c event
    process.on('SIGINT', exitHandler.bind(null, {exit:true}));
    //catches uncaught exceptions
    process.on('uncaughtException', exitHandler.bind(null, {exit:true}));
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
