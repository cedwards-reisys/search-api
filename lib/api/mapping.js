
var MappingParser = function(data) {
    this.mapping = parseMapping(data);
};

function parseMapping(mapping){
    var indexToTypeToFields = {};
    for ( var index in mapping ) {
        if ( mapping.hasOwnProperty(index) ) {
            var types = mapping[index]["mappings"];
            var typeToFields = {};
            for ( var type in types ) {
                if ( types.hasOwnProperty(type) ) {
                    var fields = types[type]["properties"];
                    var fieldsFlatten = {};
                    findFieldsRecursive(fields,fieldsFlatten,"");
                    typeToFields[type] = fieldsFlatten;
                }
            }
            indexToTypeToFields[index] = typeToFields;
        }
    }
    return indexToTypeToFields;
}

function findFieldsRecursive(fields,fieldsFlatten,prefix){
    for ( var field in fields ) {
        if ( fields.hasOwnProperty(field) ) {
            var fieldMapping = fields[field];
            if ( 'type' in fieldMapping ) {
                fieldsFlatten[prefix + field] = fieldMapping;
            }
            if ( !('type' in fieldMapping) || fieldMapping.type == 'nested' ) {
                findFieldsRecursive(fieldMapping['properties'],fieldsFlatten,prefix + field + '.');
            }
        }
    }
}

MappingParser.prototype.getIndices = function() {
    return Object.keys(this.mapping);
};

MappingParser.prototype.getTypes = function(index) {
    return Object.keys(this.mapping[index]);
};

MappingParser.prototype.getFieldsForType = function(index,type) {
    return Object.keys(this.mapping[index][type]);
};

MappingParser.prototype.getFieldsForTypeWithMapping = function(index,type) {
    return this.mapping[index][type];
};

module.exports = MappingParser;
