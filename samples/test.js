var Cloudy = require('../libs/cloudy');

var cloudy = new Cloudy({
    host: '172.16.11.204',
    port: 2181,
    path: '/cloudy/test'
}, function(node) {
    return node;
}, function(client) {
    return null;
}, function() {
    cloudy.register('172.16.11.225', 8787, 1);
    cloudy.register('172.16.11.226', 8787, 1);
    cloudy.register('172.16.11.227', 8787, 1);
    cloudy.register('172.16.11.228', 8787, 1);
    cloudy.register('172.16.11.229', 8787, 1);
    for (var i = 0; i < 100; i++) {
        console.log(cloudy.client().host);
    }
    cloudy.unregister('172.16.11.225', 8787, 1);
    cloudy.unregister('172.16.11.226', 8787, 1);
    cloudy.unregister('172.16.11.227', 8787, 1);
    cloudy.unregister('172.16.11.228', 8787, 1);
    cloudy.unregister('172.16.11.229', 8787, 1);
});
