'use strict';

class Sql {

    constructor() {}

    getString(params) {

        var query = 'SELECT ';

        // $select
        var fields = '*';
        if ( params['$select'] ) {
            fields = params['$select'];
        }

        query += query + fields + ' FROM ' + params.source + ' ';

        // $where
        if ( params['$where'] ) {
            query += params['$where'] + ' ';
        }

        if ( params['$limit'] ) {
            query += 'LIMIT ' + params['$limit'];
        }


        return query;
    }

    getObject(params) {

    }

}

module.exports = new Sql();