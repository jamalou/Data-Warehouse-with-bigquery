"use strict";
const mysql = require('mysql2/promise');
const moment = require('moment');
const AWS = require('aws-sdk');
AWS.config.update();
const s3  = new AWS.S3();

let config = require('./config.json');

exports.handler = async (event, context) => {

    try {
        console.log(config.key)
        let data = await processEvent(event, config.table);
        context.succeed(data);
    } catch (e) {
        console.log(e);
        context.done(e);
    }
};

let processEvent = async (event, table) => {

   let now = moment(); 

   // This will provide a name for your file to save in S3
   let datePrefix = now.format("YYYY/MM/DD/HH/");
   let fileKey = now.format("mm").toString();

   // This will create a connection to your database:
   let pool = mysql.createPool({
      host: config.dbInfo.host,
      user: config.dbInfo.username,
      password: config.dbInfo.password,
      waitForConnections: true,
      connectionLimit:10,
      queueLimit:0,
      timezone: '+00:00'
   });

   const result = await getResult(config.table.query, pool);


   // Now let's save MySQL db data to our s3 bucket:
   let params_MySQL = {
      Bucket: config.save_bucket,
      Key: config.key + table.name + '/' + datePrefix + table.name + fileKey,
      Body: JSON.stringify(result)
   };
   
   // add try catch error block:
   try {
         await s3.upload(params_MySQL).promise();
      
   } catch (e) {
         console.log(e);          
   }


   return {'Successfully saved ': `${result}`}
}
;

async function getResult(sql, pool){
    const result = await pool.query(sql);
    return result[0];
}
;