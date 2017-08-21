// var MongoDBClient = require('./clients/mongodb');
const Promise = require('bluebird');
const DynamoDBClient = require('./clients/dynamodb');
var RethinkDBClient = require('./clients/rethinkdb');
var validateOptions = require('./validate-options');

var importDatabase = function (options) {
    // 1. Validate Options
    options = validateOptions(options);

    var importLog = {
      logs: []
    };

   var log = function (message) {
     var time = new Date();
     if (options.log) {
       console.log('mtr : ' + time + ' : ' + message);
     }
     importLog.logs.push(time + ' : ' + message);
   };

    // 2. Instanstiate Drivers
    var sourceDB = (function () {
        if (options.source === 'rethinkdb') {
          return new RethinkDBClient(options.rethinkdb, { sourceOrTarget: 'source' });
        }
        if (options.source === 'mongodb') {
          return new MongoDBClient(options.mongodb, { sourceOrTarget: 'source' });
        }
        if (options.source === 'dynamodb') {
          throw new Error('Use DynamoDB as migration source is not supported this time');
        }
    }());
    var targetDB = (function () {
        if (options.target === 'rethinkdb') {
          return new RethinkDBClient(options.rethinkdb, { sourceOrTarget: 'target' });
        }
        if (options.target === 'mongodb') {
          return new MongoDBClient(options.mongodb, { sourceOrTarget: 'target' });
        }
        if (options.target === 'dynamodb') {
          return new DynamoDBClient(options.dynamodb, { sourceOrTarget: 'target' });
        }
    }());

    var insertionLatencyPromiseFn;
    if (options.insertionLatency > 0) {
      insertionLatencyPromiseFn = () => new Promise(resolve => setTimeout(resolve, options.insertionLatency));
    } else {
      insertionLatencyPromiseFn = () => Promise.resolve();
    }

    // Return a promise
    return Promise.resolve()
      .then(function () {
         log('Connecting To Source DB');
          // 3. Connect to Both Databases
          return sourceDB.connect()
            .then(function () {
              log('Connecting to Target DB');
              return targetDB.connect();
            });
      })
      .then(function () {
        log('Getting Tables from source database');
        // 4. Get a list of tables from the source database (if `collections` is not defined)/Create Tables in target database
        return sourceDB.getTables()
          .then(function (tables) {
            importLog.tables = (function () {
              var obj = {};
              tables.forEach(function (table) {
                obj[table.name] = table;
              });
              return obj;
            }());

            // TODO skip table creation
            // return targetDB.createTables(tables)
            //   .then(function () {

            // whitelist & blacklist
            if (options.whitelist && options.whitelist.length > 0) {
              tables = tables.filter(table => options.whitelist.indexOf(table.name) >= 0);
            }
            if (options.blacklist && options.blacklist.length > 0) {
              tables = tables.filter(table => options.blacklist.indexOf(table.name) < 0);
            }

            var recursiveInsertTable = function(numberOfTables, currentTableIndex) {
              if (currentTableIndex >= numberOfTables) return true;
              const tableConfig = tables[currentTableIndex];
              return sourceDB.getNumberOfRows(tableConfig)
                .then(function(numberOfRows) {
                  log('Starting to Insert Rows into `' + tableConfig.name + '`');
                  importLog.tables[tableConfig.name].numberOfRows = numberOfRows;

                  // Recursive Function
                  var recursiveInsertRow = function (numberOfRows, currentRow) {
                    // Finish
                    if (currentRow >= numberOfRows) {
                      return true;
                    }
                    var rowsToInsert = options.rowsPerBatch;
                    if ((numberOfRows - (currentRow + rowsToInsert)) < 0) {
                      rowsToInsert = numberOfRows - currentRow;
                    }
                    console.log('Table `' + tableConfig.name +
                      '` : Row (' + (currentRow + 1) +
                      ' - ' + rowsToInsert +
                      ')/' + numberOfRows);
                    return sourceDB.getRows(tableConfig, rowsToInsert, currentRow)
                      .then(function (rows) {
                        // This is RethinkDB to MongoDB specific
                        // This functions should be part of the logic of the client, not this ...
                        rows = rows.map(sourceDB.mapExportedRow.bind(sourceDB, tableConfig));
                        rows = rows.map(targetDB.mapImportedRow.bind(targetDB, tableConfig));
                        return targetDB.insertRows(tableConfig, rows);
                      })
                      .then(insertionLatencyPromiseFn)
                      .then(() => recursiveInsertRow(numberOfRows, currentRow + options.rowsPerBatch));
                  };
                  // 5. Start recursive importing
                  return recursiveInsertRow(numberOfRows, options.rowOffset);
                })
                .then(insertionLatencyPromiseFn)
                .then(() => recursiveInsertTable(numberOfTables, currentTableIndex + 1));
            }
            return recursiveInsertTable(tables.length, 0);
            // });
        });
    })
    .then(function () {
      return Promise.all([
        sourceDB.closeConnection(),
        targetDB.closeConnection()
      ]);
    })
    .then(function () {
      log('Import Completed');
      return importLog;
    });
};

module.exports = importDatabase;
