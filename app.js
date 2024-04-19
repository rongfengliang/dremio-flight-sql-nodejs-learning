const { createFlightSqlClient } = require('@lakehouse-rs/flight-sql-client');
const { tableFromIPC } = require('apache-arrow');

BigInt.prototype.toJSON = function() { return this.toString() }

const options = {
  username: 'admin',
  password: 'admin123',
  tls: false,
  host: 'localhost',
  port: 32010,
  headers: [],
};

async function start(){
  const client = await createFlightSqlClient(options);

  const buffer = await client.query('select * from sys.options');
  const table = tableFromIPC(buffer);
  
  table.toArray().forEach((row) => {
    console.log(JSON.stringify(row));
  })
  
  const bufferv2 = await client.getTables({ includeSchema: false });
  const tablev2  = tableFromIPC(bufferv2);
  
  tablev2.toArray().forEach((row) => {
    console.log(JSON.stringify(row));
  })  
}

start();

