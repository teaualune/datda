const datda = require('./lib/datda');

datda({
    source: 'rethinkdb',
    target: 'dynamodb',
    dynamodb: {
        credentialFilePath: process.env.CREDENTIAL_FILE_PATH || './config.json',
        region: process.env.REGION || 'ap-northeast-1',
        tableNamePrefix: process.env.TABLE_NAME_PREFIX || 'Staging_'
    },
    rethinkdb: {
        host: process.env.RDB_HOST || 'localhost',
        port: process.env.RDB_PORT || 28015,
        db: process.env.RDB_DB || 'demo'
    },
    insertionLatency: 1000,
    rowsPerBatch: 25,
    rowOffset: 0,
    tableConfig: {
        users: {
            rowOffset: 0,
            requiredFields: ['id'] // skip rows without id field
        }
    },
    blacklist: [],
    whitelist: []
})
.then(function(importLog) {
    console.log(importLog);
}, function(err) {
    console.error(err);
});
