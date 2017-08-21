'use strict';

var Promise = require('bluebird');
const aws = require('aws-sdk');
var checkType = require('../check-type');

var DynamoDBClient = function (connectionOpts, opts) {
    aws.config.loadFromPath(connectionOpts.credentialFilePath);
    this.dynamodb = Promise.promisifyAll(new aws.DynamoDB({
        region: connectionOpts.region,
        apiVersion: 'latest'
    }));
    this.tableNamePrefix = connectionOpts.tableNamePrefix || '';
  checkType(opts.sourceOrTarget, { types: ['string'], values: ['source', 'target'] });
  this.direction = opts.sourceOrTarget;
};

/*!
 * Connect to DynamoDB
 * @return <Promise>
 */
DynamoDBClient.prototype.connect = function () {
    return Promise.resolve();
};

/*!
 * Close connection
 * @return <Promise>
 */
DynamoDBClient.prototype.closeConnection = function () {
  return Promise.resolve();
};

/*!
 * Get all collections/tables in a database
 * @return <Promise>
 */
DynamoDBClient.prototype.getTables = function () {
    this.dynamodb.listTablesAsync({}).then(data => {
        return Promise.all(data.TableNames.filter(TableName => {
            return TableName.indexOf(this.tableNamePrefix) === 0;
        }).map(TableName => {
            console.log(`describing table: ${TableName}`)
            return this.dynamodb.describeTableAsync({
                TableName
            }).then(data => {
                const primaryKeys = data.Table.KeySchema.filter(keySchema => keySchema.KeyType === 'HASH');
                return {
                    name: data.Table.TableName,
                    primaryKey: primaryKeys[0].AttributeName
                };
            });
        }));
    });
};

/*!
 * Create all tables through an object with a `name` property and a `indexes` property
 * @param <Array>
 * @return <Promise>
 */
DynamoDBClient.prototype.createTables = function (collections) {
    return Promise.all(collections.map(collection => this.dynamodb.createTableAsync({
        TableName: this.tableNamePrefix + collection.name,
        KeySchema: [{
            AttributeName: collection.primaryKey,
            KeyType: 'HASH'
        }]
    })));
};

/*!
 * Get number of rows in a table
 * @param <String>
 * @return <Number>
 */
DynamoDBClient.prototype.getNumberOfRows = function (collectionConfig) {
    return this.dynamodb.describeTableAsync({
        TableName: this.tableNamePrefix + collectionConfig.name
    }).then(data => data.Table.ItemCount); 
};

/*!
 * Get rows from a collection
 * @param <String>
 * @param <Number>
 * @param <Number>
 * @return <Promise> --> <Array>
 */
DynamoDBClient.prototype.getRows = function (collectionConfig, numberOfRows, offset) {
    throw new Error('not implemented');
};

/*!
 * Map row to be exported to another database from DynamoDB
 * @param <Object>
 * @returns <Object>
 */
DynamoDBClient.prototype.mapExportedRow = function (tableConfig, row) {
    throw new Error('not implemented');
};

/*
 * Map row to be imported from another database into DynamoDB
 * @param <Object>
 * @return <Object>
 */
DynamoDBClient.prototype.mapImportedRow = function (tableConfig, row) {
  // TODO nothing to map
  return row;
};

/*!
 * Insert rows into a collection
 * @param <String>
 * @param <Array>
 * @return <Promise>
 */
DynamoDBClient.prototype.insertRows = function (collectionConfig, rows) {
    // console.log(`inserting rows`, collectionConfig, rows);
    const params = {
        RequestItems: {
            [this.tableNamePrefix + collectionConfig.name]: rows.map(row => ({
                PutRequest: {
                    Item: Object.keys(row).reduce((retval, key) => {
                        if (key === 'id') return retval;
                        let value = row[key];
                        let typeKey;
                        if (value instanceof Date) {
                            typeKey = 'N';
                            value = value / 1;
                        } else if (/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z$/.test(value)) {
                            typeKey = 'N';
                            value = (new Date(value)) / 1;
                        } else if (typeof value === 'number') {
                            typeKey = 'N';
                        } else if (typeof value === 'string') {
                            typeKey = 'S';
                        } else {
                            typeKey = 'S';
                            value = JSON.stringify(value);
                        }
                        value = `${value}`;
                        if (value.length > 0) {
                            retval[key] = {
                                [typeKey]: value
                            };
                        }
                        return retval;
                    }, {
                        // TODO assume row.id is primary key
                        [collectionConfig.primaryKey]: {
                            S: row.id
                        }
                    })
                }
            }))
        }
    };
    return this.dynamodb.batchWriteItemAsync(params);
};

module.exports = DynamoDBClient;
