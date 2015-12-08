'use strict';

const SqlHandler = require('../../lib/api/sql');
const Wreck = require('wreck');
const ResultHandler = require('../../lib/api/result');
const _ = require('lodash');


exports.register = function (server, options, next) {

    server.route({
        method: 'GET',
        path: '/',
        handler: (request, reply) => {
            reply({ message: 'Search API is accepting connections.' });
        }
    });

    server.route({
        method: 'GET',
        path: '/search/{source}',
        handler: (request, reply) => {
            const params = request.query;
            params.source = request.params.source;

            const isCsvRequest = (request.headers.accept && request.headers.accept == 'text/csv');
            const isDownloadRequest = (typeof params.download !== 'undefined');

            Wreck.get('http://127.0.0.1:9200/_sql?sql=' + SqlHandler.getString(params),{headers:{'Accept':'application/json'},json:true}, (err, res, payload) => {
                var result = {};
                if ( isCsvRequest ) {
                    let handler = ResultHandler.create(payload,true);
                    let body = handler.getBody();

                    // convert to csv
                    let csv = [];
                    let recordCount = body.length;
                    for ( let i = 0; i < recordCount; i++ ) {
                        if ( i == 0 ) {
                            csv.push(_.keys(body[i]).join(',')); // header
                        }
                        csv.push(_.values(body[i]).join(','));
                    }
                    result = csv.join("\n");
                } else {

                    let handler = ResultHandler.create(payload,false);
                    result = handler.getBody();
                }

                const response = reply(err,result);

                if ( isCsvRequest ) {
                    response.type('text/csv');
                } else {
                    response.type('application/json');
                }

                if ( isDownloadRequest ) {
                    response.header('Content-Disposition','attachment; filename=data.'+((isCsvRequest)?'csv':'json'));
                }
            });
        }
    });

    server.route({
        method: 'GET',
        path: '/sql',
        handler: (request, reply) => {
            const params = request.query;
            params.source = request.params.source;

            const isCsvRequest = (request.headers.accept && request.headers.accept == 'text/csv');
            const isDownloadRequest = (typeof params.download !== 'undefined');

            Wreck.get('http://127.0.0.1:9200/_sql?sql=' + params.q,{headers:{'Accept':'application/json'},json:true}, (err, res, payload) => {
                var result = {};
                if ( isCsvRequest ) {
                    let handler = ResultHandler.create(payload,true);
                    let body = handler.getBody();

                    // convert to csv
                    let csv = [];
                    let recordCount = body.length;
                    for ( let i = 0; i < recordCount; i++ ) {
                        csv.push(body[i].join(','));
                    }
                    result = csv.join("\n");
                } else {

                    let handler = ResultHandler.create(payload,false);
                    result = handler.getBody();
                }

                const response = reply(err,result);

                if ( isCsvRequest ) {
                    response.type('text/csv');
                } else {
                    response.type('application/json');
                }

                if ( isDownloadRequest ) {
                    response.header('Content-Disposition','attachment; filename=data.'+((isCsvRequest)?'csv':'json'));
                }
            });
        }
    });

    next();
};


exports.register.attributes = {
    name: 'api'
};
