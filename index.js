const sql = require('mysql');
const path = require('path');
const fs = require('fs');
const { convertCSVToArray } = require('convert-csv-to-array');
const settingsKeys = ['connection', 'inFile', 'outFile', 'pre', 'post', 'stmtFile', 'errLevel', 'seq'];
const newLine = require('os').EOL;
let settings;
let errLevel = 0;
let seq = false;
let connection;
(async() => {
    settings = JSON.parse(fs.readFileSync(path.join(__dirname, '/config.json')).toString('utf-8'));
        const keys = Object.keys(settings);
        for (const v of keys) {
            if (settingsKeys.indexOf(v) === -1) {
                throw new Error();
            }
        }
        if (settings.hasOwnProperty('errLevel')) {
            errLevel = settings.errLevel;
        }
        if (settings.hasOwnProperty('seq')) {
            seq = settings.seq;
        }
        connection = sql.createConnection(settings.connection);
        connection.connect();
        console.log('connected to database!');
        if (settings.hasOwnProperty('pre')) {
            const pre = fs.readFileSync(path.join(__dirname, '/' + settings.pre)).toString('utf-8');
            await new Promise((resolve, reject) => {
                connection.query(pre, (err, results) => {
                    if (err) {
                        handleError(err);
                    }
                    if (settings.hasOwnProperty('outFile')) {
                        fs.writeFileSync(path.join(__dirname, '/' + settings.outFile + ' pre script.txt'), JSON.stringify(results, null, 4) + '\r\n', { flag: 'a' });
                        resolve();
                    }
                });
            });
            console.log('Pre script done!');
        }
        if (typeof settings.inFile === 'string') {
            await handleFile(settings.inFile);
            console.log(`Done inserting for file: ${settings.inFile}`);
        } else {
            for (const v of settings.inFile) {
                if (seq) {
                    await handleFile(v);
                } else {
                    handleFile(v);
                }
                console.log(`Done inserting for file: ${v}`);
            }
        }
        console.log('Data insertion complete!');
        if (settings.hasOwnProperty('post')) {
            const pre = fs.readFileSync(path.join(__dirname, '/' + settings.post)).toString('utf-8');
            await new Promise((resolve, reject) => {
                connection.query(pre, (err, results) => {
                    if (err) {
                        handleError(err);
                    }
                    if (settings.hasOwnProperty('outFile')) {
                        fs.writeFileSync(path.join(__dirname, '/' + settings.outFile + ' post script.txt'), JSON.stringify(results, null, 4) + '\r\n', { flag: 'a' });
                        resolve();
                    }
                });
            });
            console.log('Post script done!');
        }
        connection.end();
        console.log('Connection closed!');
})();
function handleError(err) {
    switch (errLevel) {
        case 0: {
            console.log(err);
            break;
        }
        case 1: {
            throw err;
            break;
        }
    }
}
async function handleFile(fileName) {
    const data = fs.readFileSync(path.join(__dirname, '/' + fileName)).toString();
    const dataArray = convertCSVToArray(data, { type: 'array', separator: ',' });
    const headers = dataArray[0];
    const table = fileName.slice(0, fileName.length - 4);
    let headerStmt = '';
    if (headers.length === 0) {
        handleError(new Error('Empty headers!'));
    }
    for (const v of headers) {
        headerStmt += (v.trim() + ', ');
    }
    headerStmt = headerStmt.slice(0, -2);
    dataArray.shift();
    const statements = [];
    const results = [];
    if (seq) {
        for (const v of dataArray) {
            const stmt = `INSERT INTO ${table} (${headerStmt}) VALUES(${genInserts(v)});`;
            statements.push(stmt);
            await new Promise((resolve, reject) => {
                connection.query(stmt, (err, result) => {
                    if (err) handleError(err);
                    results.push(result);
                    resolve();
                });
            });
        }
    } else {
        await new Promise((resolve, reject) => {
            const numInserts = dataArray.length;
            let count = 0;
            for (const v of dataArray) {
                const stmt = `INSERT INTO ${table} (${headerStmt}) VALUES(${genInserts(v)});`;
                statements.push(stmt);
                connection.query(stmt, (err, result) => {
                    if (err) handleError(err);
                    count++;
                    results.push(result);
                    if (count === numInserts) {
                        resolve();
                    }
                });
            }
        });
    }
    if (settings.hasOwnProperty('outFile')) {
        fs.writeFileSync(path.join(__dirname, '/' + settings.outFile + '.txt'), JSON.stringify(results, null, 4) + '\r\n', { flag: 'a' });
    }
    if (settings.hasOwnProperty('stmtFile')) {
        fs.writeFileSync(path.join(__dirname, '/' + settings.stmtFile + '.sql'), getStatements(statements), { flag: 'a' });
    }
    return;
}
function getStatements(statements) {
    let str = '';
    for (const v of statements) {
        str += (v + newLine);
    }
    return str;
}
function genInserts(items) {
    let str = '';
    for (const v of Object.keys(items)) {
        if (typeof items[v] === 'string') {
            str += (connection.escape(items[v].replace(/\n|\r/g, '')) + ', ');
        } else {
            str += (connection.escape(items[v]) + ', ');
        }
    }
    str = str.slice(0, -2);
    return str;
}