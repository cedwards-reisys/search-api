'use strict';

const SqlHelper = require('../../lib/api/sql');

exports.register = function (server, options, next) {

    server.route({
        method: 'GET',
        path: '/',
        handler: function (request, reply) {

            reply({ message: 'Search API is accepting connections.' });
        }
    });

    server.route({
        method: 'GET',
        path: '/search/{index}',
        handler: function (request, reply) {

            reply(
                {
                    index: request.params.index,
                    query: request.query,
                    sql: SqlHelper.getString(request.query)
                }
            );
        }
    });

    next();
};


exports.register.attributes = {
    name: 'api'
};
