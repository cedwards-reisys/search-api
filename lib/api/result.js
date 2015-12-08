var MappingParser = require('./mapping');
var _ = require('lodash');

var ResultHandlerFactory = {
    "create": function(data,isFlat) {
        function isSearch(){
            return "hits" in data
        }
        // Is query is of aggregation type? (SQL group by)
        function isAggregation() {
            return "aggregations" in data
        }
        function isDelete(){
            return "_indices" in data
        }

        if(isSearch()){
            return isAggregation() ? new AggregationQueryResultHandler(data) : new DefaultQueryResultHandler(data,isFlat)
        }

        if(isDelete()){
            return new DeleteQueryResultHandler(data);
        }
        return new ShowQueryResultHandler(data);

    }
};

/* DefaultQueryResultHandler object
 Handle the query result,
 in case of regular query
 (Not aggregation)
 */
var DefaultQueryResultHandler = function(data,isFlat) {

    // createScheme by traverse hits field
    function createScheme() {
        var hits = data.hits.hits;
        var scheme = [];
        var hitCount = hits.length;
        for(var index=0; index<hitCount; index++) {
            var hit = hits[index];
            var header = _.extend({},hit['_source'],hit.fields);
            if ( isFlat ) {
                findKeysRecursive(scheme,header,"");
            } else {
                for(var key in header) {
                    if (header.hasOwnProperty(key)) {
                        if(scheme.indexOf(key) == -1) {
                            scheme.push(key);
                        }
                    }
                }
            }
        }
        return scheme;
    }

    this.data = data;
    this.head = createScheme();
    this.isFlat = isFlat;
    this.scrollId = data["_scroll_id"];
    this.isScroll = this.scrollId!=undefined && this.scrollId!="";
};

DefaultQueryResultHandler.prototype.isScroll = function() {
    return this.isScroll;
};

DefaultQueryResultHandler.prototype.getScrollId = function() {
    return this.scrollId;
};

DefaultQueryResultHandler.prototype.getHead = function() {
    return this.head;
};

DefaultQueryResultHandler.prototype.getBody = function() {
    var hits = this.data.hits.hits;
    var body = [];
    var hitCount = hits.length;
    for(var i = 0; i < hitCount; i++) {
        var row = hits[i]['_source'];

        if ( hits[i].hasOwnProperty('fields') ) {
            addFieldsToRow(row,hits[i]);
        }

        if ( this.isFlat ) {
            row = flatRow(this.head,row);
        }
        body.push(row);
    }
    return body
};

DefaultQueryResultHandler.prototype.getTotal = function() {
    return this.data.hits.total;
};

DefaultQueryResultHandler.prototype.getCurrentHitsSize = function() {
    return this.data.hits.hits.length;
};

function findKeysRecursive (scheme,keys,prefix) {
    for ( var key in keys ){
        if (keys.hasOwnProperty(key)) {
            if ( typeof(keys[key]) == 'object' && (!(keys[key] instanceof Array)) ) {
                findKeysRecursive(scheme,keys[key],prefix + key + '.')
            } else {
                if ( scheme.indexOf(prefix + key) == -1 ) {
                    scheme.push(prefix + key);
                }
            }
        }
    }
}

function flatRow (keys,row) {
    var flattenRow = {};
    var keyCount = keys.length;
    for ( var i = 0 ; i< keyCount; i++ ){
        var key = keys[i];
        var splitKey = key.split(".");
        var found = true;
        var currentObj = row;
        var splitKeyLength = splitKey.length;
        for ( var j = 0; j < splitKeyLength; j++){
            if ( currentObj[splitKey[j]] == undefined ){
                found = false;
                break;
            } else {
                currentObj = currentObj[splitKey[j]];
            }
        }
        if ( found ) {
            flattenRow[key] = currentObj;
        }
    }
    return flattenRow;
}

function addFieldsToRow (row,hit) {
    for ( var field in hit.fields ) {
        if (hit.fields.hasOwnProperty(field)) {
            var fieldValue = hit.fields[field];
            if (fieldValue instanceof Array) {
                if (fieldValue.length > 1) {
                    row[field] = fieldValue;
                } else {
                    row[field] = fieldValue[0];
                }
            } else {
                row[field] = fieldValue;
            }
        }
    }
}

function removeNestedAndFilters (aggs) {
    for ( var field in aggs ) {
        if ( aggs.hasOwnProperty(field) ) {
            if (field.endsWith("@NESTED") || field.endsWith("@FILTER")){
                delete aggs[field]["doc_count"];
                delete aggs[field]["key"];
                var leftField = Object.keys(aggs[field])[0];
                aggs[leftField] = aggs[field][leftField];
                delete aggs[field];
                removeNestedAndFilters(aggs);
            }
            if (typeof(aggs[field])=="object") {
                removeNestedAndFilters(aggs[field]);
            }
        }
    }
}

/* AggregationQueryResultHandler object
 Handle the query result,
 in case of Aggregation query
 (SQL group by)
 */
var AggregationQueryResultHandler = function(data) {
    removeNestedAndFilters(data['aggregations']);
    function getRows(bucketName, bucket, additionalColumns) {
        var rows = [];
        var subBuckets = getSubBuckets(bucket);
        var subBucketsLength = subBuckets.length;
        if ( subBucketsLength > 0 ) {
            for ( var i = 0; i < subBucketsLength; i++) {
                var subBucketName = subBuckets[i]["bucketName"];
                var subBucket = subBuckets[i]["bucket"];
                var newAdditionalColumns = {};
                // bucket without parents.
                if ( bucketName != undefined) {
                    var newColumn = {};
                    newColumn[bucketName] = bucket.key;
                    newAdditionalColumns = _.extend(newColumn, additionalColumns);
                }
                rows = rows.concat(getRows(subBucketName, subBucket, newAdditionalColumns));
            }
        } else {
            var obj = _.extend({}, additionalColumns);
            if ( bucketName != undefined ) {
                if ( bucketName != undefined ) {
                    if("key_as_string" in bucket){
                        obj[bucketName] = bucket["key_as_string"]
                    } else {
                        obj[bucketName] = bucket.key
                    }
                }
            }

            for ( var field in bucket ) {
                if ( bucket.hasOwnProperty(field) ) {
                    var bucketValue = bucket[field];
                    if ( bucketValue['buckets'] != undefined ){
                        rows = rows.concat(getRows(subBucketName, bucketValue, newAdditionalColumns));
                        continue;
                    }
                    if ( bucketValue.value != undefined) {
                        if("value_as_string" in bucket[field]){
                            obj[field] = bucketValue["value_as_string"]
                        } else {
                            obj[field] = bucketValue.value
                        }
                    } else {
                        if ( typeof(bucketValue) == "object" ) {
                            /*subBuckets = getSubBuckets(bucketValue);
                             if(subBuckets.length >0){
                             var newRows = getRows(subBucketName, {"buckets":subBuckets}, newAdditionalColumns);
                             rows = rows.concat(newRows);
                             continue;
                             }*/
                            fillFieldsForSpecificAggregation(obj,bucketValue,field);
                        }
                    }
                }

            }
            rows.push(obj)
        }

        return rows
    }

    function fillFieldsForSpecificAggregation ( obj, value, field ) {
        for ( var key in value){
            if ( value.hasOwnProperty(key) ) {
                if ( key == "values" ) {
                    fillFieldsForSpecificAggregation(obj,value[key],field);
                } else {
                    obj[field+"." +key] = value[key];
                }
            }
        }
    }

    function getSubBuckets ( bucket ) {
        var subBuckets = [];
        for( var field in bucket) {
            if ( bucket.hasOwnProperty(field) ) {
                var buckets = bucket[field]['buckets'];
                if ( buckets != undefined ) {
                    var bucketsLength = buckets.length;
                    for ( var i = 0; i < bucketsLength; i++) {
                        subBuckets.push({"bucketName": field, "bucket": buckets[i]})
                    }
                } else {
                    var innerAgg = bucket[field];
                    for ( var innerField in innerAgg ) {
                        if ( innerAgg.hasOwnProperty(innerField) ) {
                            if ( typeof(innerAgg[innerField]) == "object" ) {
                                subBuckets = subBuckets.concat(getSubBuckets(innerAgg[innerField]));
                            }
                        }
                    }
                }
            }
        }
        return subBuckets
    }

    this.data = data;
    this.flattenBuckets = getRows(undefined, data['aggregations'], {})
};

AggregationQueryResultHandler.prototype.getHead = function() {
    var head = [];
    var flattenBucketsLength = this.flattenBuckets.length;
    for ( var i = 0; i < flattenBucketsLength; i++) {
        var keys = Object.keys(this.flattenBuckets[i]);
        for ( var j = 0; j < keys.length; j++ ) {
            if ( _.indexOf(head,keys[j]) == -1) {
                head.push(keys[j]);
            }
        }
    }
    return head
};

AggregationQueryResultHandler.prototype.getBody = function() {
    return this.flattenBuckets;
};


AggregationQueryResultHandler.prototype.getTotal = function() {
    return undefined;
};

AggregationQueryResultHandler.prototype.getCurrentHitsSize = function() {
    return this.flattenBuckets.length;
};

/* ShowQueryResultHandler object
 for showing mapping in some levels (cluster, index and types)
 */
var ShowQueryResultHandler = function(data) {

    var mappingParser = new MappingParser(data);
    var indices = mappingParser.getIndices();
    var body = [];
    if ( indices.length > 1 ){
        this.head = ["index","types"];
        for ( var indexOfIndex in indices ) {
            if ( indices.hasOwnProperty(indexOfIndex) ) {
                var indexToTypes = {};
                indexToTypes["index"] = indices[indexOfIndex];
                indexToTypes["types"] = mappingParser.getTypes(indices[indexOfIndex]);
                body.push(indexToTypes);
            }
        }
    } else {
        var index = indices[0];
        var types = mappingParser.getTypes(index);
        if ( types.length > 1 ) {
            this.head = ["type","fields"];
            for ( var typeIndex in types ) {
                if ( types.hasOwnProperty(typeIndex) ) {
                    var typeToFields = {};
                    typeToFields["type"] = types[typeIndex];
                    typeToFields["fields"] = mappingParser.getFieldsForType(index,types[typeIndex]);
                    body.push(typeToFields);
                }
            }
        } else {
            this.head = ["field","type"];
            var anyFieldContainsMore = false;
            var fieldsWithMapping = mappingParser.getFieldsForTypeWithMapping(index,types[0]);
            for ( var field in fieldsWithMapping ) {
                if ( fieldsWithMapping.hasOwnProperty(field) ) {
                    var fieldMapping = fieldsWithMapping[field];
                    var fieldRow = {
                        field: field,
                        type: fieldMapping['type']
                    };
                    delete fieldMapping['type'];
                    if ( !_.isEmpty(fieldMapping) ) {
                        anyFieldContainsMore = true;
                        fieldRow['more'] = fieldMapping;
                    }
                    body.push(fieldRow);
                }
            }
            if ( anyFieldContainsMore ) { this.head.push("more"); }
        }
    }

    this.body = body;
};


ShowQueryResultHandler.prototype.getHead = function() {
    return this.head;
};

ShowQueryResultHandler.prototype.getBody = function() {
    return this.body;
};

ShowQueryResultHandler.prototype.getTotal = function() {
    return this.body.length;
};

ShowQueryResultHandler.prototype.getCurrentHitsSize = function() {
    return this.body.length;
};

/* DeleteQueryResultHandler object
 to show delete result status
 */
var DeleteQueryResultHandler = function(data) {
    this.head = ["index_deleted_from","shards_successful","shards_failed"];
    var body = [];
    var deleteData = data["_indices"];
    for ( var index in deleteData ) {
        if ( deleteData.hasOwnProperty(index) ) {
            var deleteStat = {};
            deleteStat["index_deleted_from"] = index;
            var shardsData = deleteData[index]["_shards"];
            deleteStat["shards_successful"] = shardsData["successful"];
            deleteStat["shards_failed"] = shardsData["failed"];
            body.push(deleteStat);
        }
    }
    this.body = body;
};


DeleteQueryResultHandler.prototype.getHead = function() {
    return this.head;
};

DeleteQueryResultHandler.prototype.getBody = function() {
    return this.body;
};

DeleteQueryResultHandler.prototype.getTotal = function() {
    return 1;
};

DeleteQueryResultHandler.prototype.getCurrentHitsSize = function() {
    return 1;
};

module.exports = ResultHandlerFactory;
