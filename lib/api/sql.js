'use strict';

class Sql {

    constructor() {}

    getString(params) {

        let query = 'SELECT ';

        // $select
        let fields = '*';
        if ( params['$select'] ) {
            fields = params['$select'];
        }

        query += fields + ' FROM ' + params.source + ' ';

        // $where
        if ( params['$where'] ) {
            query += 'WHERE ' + params['$where'] + ' ';
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
