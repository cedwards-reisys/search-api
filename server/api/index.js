'use strict';

exports.register = function (server, options, next) {

    server.route({
        method: 'GET',
        path: '/',
        handler: function (request, reply) {

            reply({ message: 'Search API is accepting connections.' });
        }
    });


    next();
};


exports.register.attributes = {
    name: 'api'
};
