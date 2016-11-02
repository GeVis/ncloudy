var Cloudy = require('../libs/cloudy');

var cloudy = new Cloudy({
    host: '172.16.11.204',
    port: 2181,
    path: '/heatmap/test/test'
}, function(node) {
    console.log('test create', node)
    return node;
}, function(client) {
    console.log('test delete', client)
    return null;
}, function(err, clients) {
    console.log('test ready', clients)
    var port = ~~(Math.random()*100);
    cloudy.register('172.16.11.225', port, 1);
    // cloudy.register('172.16.11.226', 8787, 1);
    // cloudy.register('172.16.11.227', 8787, 1);
    // cloudy.register('172.16.11.228', 8787, 1);
    // cloudy.register('172.16.11.229', 8787, 1);
    // for (var i = 0; i < 20; i++) {
    //     var client = cloudy.client()
    //     console.log(client.host, client.port);
    // }
    // setTimeout(function(){
    //     cloudy.unregister('172.16.11.225', port, 1);
    //     // cloudy.unregister('172.16.11.226', 8787, 1);
    //     // cloudy.unregister('172.16.11.227', 8787, 1);
    //     // cloudy.unregister('172.16.11.228', 8787, 1);
    //     // cloudy.unregister('172.16.11.229', 8787, 1);
    // }, 5000)
});
