const minimist = require('minimist');
const Aws = require('aws-sdk');
var unmarshalItem = require('dynamodb-marshaler').unmarshalItem;
const { parse } = require('json2csv');
const fs = require('fs');

const { role, region, table, output } = validateArgs(minimist(process.argv.slice(2), {
    alias: {
        r: 'region',
        t: 'table',
        o: 'output',
    }
}));

console.info('Assuming role ', role);
const creds = new Aws.TemporaryCredentials({
    RoleSessionName: 'dynamodb-csv-export',
    RoleArn: role,
});
Aws.config.credentials = creds;
creds.refresh(error => {
    if (error) {
        terminate(error);
    } else {
        console.info(`Exporting data from table ${table} in region ${region}`);
        readTable(region, table)
            .then(records => {
                console.info(`Finished reading table and found ${records.length}`);
                const fields = records.length === 0 ? [] : Object.keys(records[0]);
                const csv = parse(records, { fields });
                fs.writeFileSync(output, csv);
                console.info(`Successfully wrote output to ${output}`);
            }).catch(error => terminate(error));
    }
});

function terminate(error) {
    if (error) {
        console.error('Terminated with error:  ', error);
    }
    process.exit(1);
}

function validateArgs(args) {
    if (!args.role) {
        terminate('role required');
    } else if (!args.region) {
        terminate('region required');
    } else if (!args.table) {
        terminate('table required');
    } else if (!args.output) {
        terminate('output required');
    }

    return args;
}

function readTable(region, table) {
    const dynamo = new Aws.DynamoDB({ region });
    let records = [];
    const readRecords = (startKey) => {
        return dynamo.scan({ TableName: table, ExclusiveStartKey: startKey }).promise().then(results => {
            console.info(`Read ${results.Count} items....`);
            records = records.concat(results.Items.map(unmarshalItem));
            return results.LastEvaluatedKey ? readRecords(results.LastEvaluatedKey) : records;
        });
    }
    return readRecords(null);
}