const express = require("express");
const request = require('request');
const mysql = require("mysql2");

const app = express();

const csvUrl = 'https://gillirondformelior.s3-eu-central-1.amazonaws.com/test_project_addresses.csv';

let getArrFromCsv = data => {
    let parsedAddresses = [];
    let lines = data.split(/[\r\n]+/);
    for (let line, i = 0, l = lines.length; i < l; i++) {
        line = lines[i].split(',');
        parsedAddresses.push(line);
    }

    return parsedAddresses;
};

let filterUniqueAddresses = arr => {
    //row1 - headers
    console.log('length non-unique ', arr.length);
    let headers = arr.shift();
    arr.map(row => JSON.stringify(row))
        .filter((val, index, self) => self.indexOf(val) === index)
        .map(str => JSON.parse(str));
    arr.unshift(headers);
    console.log('length unique ', arr.length);

    return arr;
};

let saveAddresses = arr => {
    const pool = mysql.createPool({
        connectionLimit: 10,
        host: "localhost",
        user: "root",
        database: "melior",
        password: "melior"
    }).promise();

    let headers = arr[0];
    arr.shift();

    let sql = "INSERT INTO addresses (" + headers.join(", ") + ") VALUES ?";
    /*
    *  Если нужно проверить на уникальность не строки csv-файла, а записи в БД при добавлении, то меняем строку так:
    *
    * let sql = "";
    * for (let i = 0; i < arr.length; i++) {
    *     let insert = "INSERT INTO addresses (" + headers.join(", ") + ") VALUES (" + arr[i].join(", ") + ") "
    *         + "SELECT * FROM (SELECT " + arr[i].join(", ") + ") AS tmp "
    *         + "WHERE NOT EXISTS ( "
    *         + "SELECT " + headers.join(", ") + " FROM addresses WHERE " + arr[i].map((val, index, self) => headers[index] + " = " + val).join(", ")
    *         + ") LIMIT 1";
    *     sql += insert + "; ";
    * }
    *
    *
    * */
    pool.query(sql, [arr])
        .then(res => {
            console.log("Addresses were saved!");
        })
        .then(res => {
            pool.end();
            console.timeEnd("addresses time");
        }).catch(err => {
        console.error("Error in DB: " + err.message);

    });
};

let loadCSV = (req, res) => {
    console.time("addresses time");
    request.get(csvUrl, (error, resp, body) => {
        if (!error && resp.statusCode == 200) {
            saveAddresses(filterUniqueAddresses(getArrFromCsv(body)));
        } else {
            console.error('Error loading from remote url');
        }
    });
};

app.get("/api/load-from-s3", loadCSV);

app.listen(3001, function () {
    console.log("Server is waiting for connection...");
});