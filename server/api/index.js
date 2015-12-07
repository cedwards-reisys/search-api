'use strict';

const SqlHelper = require('../../lib/api/sql');
const Wreck = require('wreck');


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
        path: '/search/{source}',
        handler: function (request, reply) {

            var params = request.query;
            params.source = request.params.source;

            Wreck.get('http://127.0.0.1:9200/_sql?sql='+SqlHelper.getString(params), function (err, res, payload) {

                reply(err,payload.toString()).type('application/json');

            });
        }
    });

    server.route({
        method: 'GET',
        path: '/sql',
        config: {

        },
        handler: function (request, reply) {

            var params = request.query;

            Wreck.get('http://127.0.0.1:9200/_sql?sql='+params['q'], function (err, res, payload) {

                reply(err,payload.toString()).type('application/json');

            });
        }
    });

    next();
};


exports.register.attributes = {
    name: 'api'
};
