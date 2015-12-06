'use strict';

class Sql {

    constructor() {}

    getString(params) {

        return 'SELECT * FROM test';

    }

    getObject(params) {

    }

}

module.exports = new Sql();